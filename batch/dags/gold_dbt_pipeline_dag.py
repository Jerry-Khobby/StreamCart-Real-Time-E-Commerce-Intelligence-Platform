"""
DAG: gold_dbt_pipeline
======================
Runs the dbt Gold layer daily at 03:00 UTC.

Execution order:
  1. dbt deps       install packages
  2. source tests   validate the Silver tables before building on them
  3. freshness      warn if Silver has stopped updating
  4. staging        views over Silver
  5. dimensions     dim_*
  6. facts          fct_*
  7. aggregates     agg_*
  8. log counts     fail if Gold is empty while Silver has data

Each layer runs `dbt build`, which tests each model straight after building it. A
failed test stops the next layer, so a bad dimension can't flow into facts and KPIs.
The failing table itself stays visible until it's fixed.

Each layer is one dbt invocation. dbt already orders and parallelises models within
it, and concurrent dbt processes would conflict over the shared target/ directory.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common import record_failure

DBT_DIR = "/opt/airflow/dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt --no-use-colors"
DBT_ARGS = f"--profiles-dir {DBT_DIR} --target dev"

GOLD_TABLES = [
    "gold.dim_customers", "gold.dim_products", "gold.dim_dates", "gold.dim_regions",
    "gold.fct_orders", "gold.fct_sessions", "gold.fct_inventory_snapshots",
    "gold.agg_daily_revenue", "gold.agg_fraud_summary", "gold.agg_inventory_health",
    "gold.agg_funnel_conversion", "gold.agg_customer_retention",
]


def build_layer(layer: str) -> BashOperator:
    return BashOperator(task_id=f"dbt_build_{layer}", bash_command=f"{DBT_CMD} build --select {layer} {DBT_ARGS}")


def log_gold_row_counts():
    """Log row counts for all Gold tables. A missing table should fail the run."""
    hook = PostgresHook(postgres_conn_id="streamcart_postgres")
    for table in GOLD_TABLES:
        count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        print(f"{table:<35} {count:>10,} rows")

    # dbt tests pass on an empty table, so check that Silver's orders actually arrived.
    silver = hook.get_first("SELECT COUNT(*) FROM silver.transactions WHERE status <> 'failed'")[0]
    gold = hook.get_first("SELECT COUNT(*) FROM gold.fct_orders")[0]
    if silver and not gold:
        raise ValueError(f"gold.fct_orders is empty but Silver has {silver:,} orders")


with DAG(
    dag_id="gold_dbt_pipeline",
    description="Daily dbt Gold layer: Silver (Postgres) → Gold dims/facts/aggs",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "streamcart",
        "on_failure_callback": record_failure,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["gold", "dbt", "streamcart"],
) as dag:

    dbt_deps = BashOperator(task_id="dbt_deps", bash_command=f"{DBT_CMD} deps {DBT_ARGS}")

    dbt_source_test = BashOperator(
        task_id="dbt_source_test",
        bash_command=f"{DBT_CMD} test --select source:silver {DBT_ARGS}",
    )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_CMD} source freshness {DBT_ARGS}",
    )

    log_counts = PythonOperator(task_id="log_gold_counts", python_callable=log_gold_row_counts)

    (
        dbt_deps
        >> dbt_source_test
        >> dbt_freshness
        >> build_layer("staging")
        >> build_layer("dimensions")
        >> build_layer("facts")
        >> build_layer("aggregates")
        >> log_counts
    )
