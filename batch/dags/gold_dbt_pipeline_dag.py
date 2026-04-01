""" 
DAG: gold_dbt_pipeline
======================
Runs dbt Gold layer daily at 03:00 UTC — after silver_pipeline finishes.

Execution order:
  1. dbt deps         — ensures packages are installed
  2. source tests     — validates Silver tables have fresh data
  3. dbt run dims     — dimension tables (parallel)
  4. dbt run facts    — fact tables (depend on dims)
  5. dbt run aggs     — aggregates (depend on facts)
  6. dbt test         — validates all Gold output
  7. log counts       — observability check
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook

DBT_DIR      = "/opt/airflow/dbt"
DBT_PROFILES = "/opt/airflow/dbt"
DBT_CMD      = f"cd {DBT_DIR} && dbt --no-use-colors --profiles-dir {DBT_PROFILES} --target dev "

default_args = {
    "owner":             "streamcart",
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

def log_gold_row_counts():
    """Log row counts for all Gold tables for observability."""
    hook = PostgresHook(postgres_conn_id="streamcart_postgres")
    tables = [
        "gold.dim_customers",
        "gold.dim_products",
        "gold.dim_dates",
        "gold.dim_regions",
        "gold.fct_orders",
        "gold.fct_sessions",
        "gold.fct_inventory_snapshots",
        "gold.agg_daily_revenue",
        "gold.agg_fraud_summary",
        "gold.agg_inventory_health",
        "gold.agg_funnel_conversion",
    ]
    print("\n" + "="*50)
    print("GOLD LAYER ROW COUNTS")
    print("="*50)
    for table in tables:
        try:
            count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
            print(f"  {table:<45} {count:>10,} rows")
        except Exception as e:
            print(f"  {table:<45} ERROR: {e}")
    print("="*50)

with DAG(
    dag_id="gold_dbt_pipeline",
    description="Daily dbt Gold layer: Silver (Postgres) → Gold dims/facts/aggs",
    schedule_interval="0 3 * * *",   # 03:00 UTC daily, after silver finishes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["gold", "dbt", "streamcart"],
) as dag:

    # Step 1 — install dbt packages
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=DBT_CMD + "deps",
    )

    # Step 2 — validate silver sources have data before running
    dbt_source_test = BashOperator(
        task_id="dbt_source_test",
        bash_command=DBT_CMD + "test --select source:silver",
    )

    # Step 3 — dimension tables (all can run in parallel)
    with TaskGroup("dimensions") as tg_dims:
        BashOperator(task_id="dim_customers", bash_command=DBT_CMD + "run --select dim_customers")
        BashOperator(task_id="dim_products",  bash_command=DBT_CMD + "run --select dim_products")
        BashOperator(task_id="dim_dates",     bash_command=DBT_CMD + "run --select dim_dates")
        BashOperator(task_id="dim_regions",   bash_command=DBT_CMD + "run --select dim_regions")

    # Step 4 — fact tables (depend on dims)
    with TaskGroup("facts") as tg_facts:
        BashOperator(task_id="fct_orders",              bash_command=DBT_CMD + "run --select fct_orders")
        BashOperator(task_id="fct_sessions",            bash_command=DBT_CMD + "run --select fct_sessions")
        BashOperator(task_id="fct_inventory_snapshots", bash_command=DBT_CMD + "run --select fct_inventory_snapshots")

    # Step 5 — aggregates (depend on facts)
    with TaskGroup("aggregates") as tg_aggs:
        BashOperator(task_id="agg_daily_revenue",    bash_command=DBT_CMD + "run --select agg_daily_revenue")
        BashOperator(task_id="agg_fraud_summary",    bash_command=DBT_CMD + "run --select agg_fraud_summary")
        BashOperator(task_id="agg_inventory_health", bash_command=DBT_CMD + "run --select agg_inventory_health")
        BashOperator(task_id="agg_funnel_conversion",bash_command=DBT_CMD + "run --select agg_funnel_conversion")

    # Step 6 — test all Gold output
    dbt_test = BashOperator(
        task_id="dbt_test_gold",
        bash_command=DBT_CMD + "test --select gold",
    )

    # Step 7 — log counts for observability
    log_counts = PythonOperator(
        task_id="log_gold_counts",
        python_callable=log_gold_row_counts,
    )

    # Pipeline dependency chain
    dbt_deps >> dbt_source_test >> tg_dims >> tg_facts >> tg_aggs >> dbt_test >> log_counts