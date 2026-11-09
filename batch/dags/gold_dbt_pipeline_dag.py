"""
DAG: gold_dbt_pipeline
======================
Runs the dbt Gold layer whenever Silver is updated, by the hourly Silver job or a
backfill. A fixed time could come before Silver had finished, and on a fresh install
Gold would build from empty tables.

Write, audit, publish: every table is built and tested in a separate schema,
gold_next, and moved into gold only when everything has passed. Superset and analysts
never see a table that failed its tests, and a failed run leaves yesterday's Gold intact.

Execution order:
  1. dbt deps       install packages
  2. source tests   validate the Silver tables before building on them
  3. freshness      warn if Silver has stopped updating
  4. prepare        empty gold_next and copy the current Gold tables into it, since
                    incremental models build on their existing rows
  5. seed           load the regions reference table
  6. staging        views over Silver
  7. dimensions     dim_*
  8. facts          fct_*
  9. aggregates     agg_*
 10. audit          row counts, and fail if Gold is empty while Silver has data
 11. publish        move every table and view into gold, in one transaction

Each layer runs `dbt build`, which tests each model straight after building it. A
failed test stops every later task, including the publish.

Copying Gold on each run is cheap at this size. A large warehouse would use a
zero-copy clone or a table-format branch instead.

Each layer is one dbt invocation. dbt already orders and parallelises models within
it, and concurrent dbt processes would conflict over the shared target/ directory.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common import SILVER_DATASET, record_failure

DBT_DIR = "/opt/airflow/dbt"
LIVE_SCHEMA = "gold"
BUILD_SCHEMA = "gold_next"  # created by migration V9
DBT_CMD = f"cd {DBT_DIR} && DBT_SCHEMA={BUILD_SCHEMA} dbt --no-use-colors"
DBT_ARGS = f"--profiles-dir {DBT_DIR} --target dev"

GOLD_TABLES = [
    "dim_customers", "dim_products", "dim_dates", "dim_regions",
    "fct_orders", "fct_sessions", "fct_inventory_snapshots",
    "agg_daily_revenue", "agg_fraud_summary", "agg_inventory_health",
    "agg_funnel_conversion", "agg_customer_retention", "agg_weather_demand",
]


def hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id="streamcart_postgres")


def relations(pg: PostgresHook, schema: str) -> list[tuple[str, str]]:
    """Tables and views in a schema, as (name, TABLE or VIEW)."""
    return pg.get_records("""
        SELECT c.relname, CASE c.relkind WHEN 'v' THEN 'VIEW' ELSE 'TABLE' END
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind IN ('r', 'p', 'v')
        ORDER BY c.relkind DESC, c.relname""", parameters=(schema,))


def prepare_statements(built: list[tuple[str, str]], live: list[tuple[str, str]]) -> list[str]:
    """SQL that empties gold_next, then copies each live table into it. Views are rebuilt by dbt."""
    statements = [f'DROP {kind} IF EXISTS {BUILD_SCHEMA}."{name}" CASCADE' for name, kind in built]
    for name, kind in live:
        if kind == "TABLE":
            statements.append(f'CREATE TABLE {BUILD_SCHEMA}."{name}" (LIKE {LIVE_SCHEMA}."{name}" INCLUDING ALL)')
            statements.append(f'INSERT INTO {BUILD_SCHEMA}."{name}" SELECT * FROM {LIVE_SCHEMA}."{name}"')
    return statements


def prepare_build_schema():
    """Empty gold_next, then copy the live tables in, so incremental models continue from them."""
    pg = hook()
    statements = prepare_statements(relations(pg, BUILD_SCHEMA), relations(pg, LIVE_SCHEMA))
    # On a fresh install both schemas are empty, and dbt builds everything from scratch
    if statements:
        pg.run(statements)  # one transaction
    print(f"Prepared {BUILD_SCHEMA} with {sum(s.startswith('CREATE') for s in statements)} tables from {LIVE_SCHEMA}")


def publish_gold():
    """Swap every built table and view into gold in one transaction: readers see all or nothing."""
    pg = hook()
    built = relations(pg, BUILD_SCHEMA)
    statements = ["SET LOCAL lock_timeout = '60s'"]
    for name, kind in built:
        statements.append(f'DROP {kind} IF EXISTS {LIVE_SCHEMA}."{name}"')
        statements.append(f'ALTER {kind} {BUILD_SCHEMA}."{name}" SET SCHEMA {LIVE_SCHEMA}')
    pg.run(statements)
    print(f"Published {len(built)} tables and views to {LIVE_SCHEMA}")


def build_layer(layer: str) -> BashOperator:
    # "buildable" holds a test back until every table it needs exists. Otherwise the
    # dimensions layer runs fct_orders' foreign key tests before fct_orders is built,
    # which fails on a fresh install.
    return BashOperator(
        task_id=f"dbt_build_{layer}",
        bash_command=f"{DBT_CMD} build --select {layer} --indirect-selection buildable {DBT_ARGS}",
    )


def audit_gold():
    """Row counts for the built tables. A missing table, or an empty Gold over a full Silver, fails the run."""
    pg = hook()
    for table in GOLD_TABLES:
        count = pg.get_first(f"SELECT COUNT(*) FROM {BUILD_SCHEMA}.{table}")[0]
        print(f"{table:<35} {count:>10,} rows")

    # dbt tests pass on an empty table, so check that Silver's orders actually arrived.
    silver = pg.get_first("SELECT COUNT(*) FROM silver.transactions WHERE status <> 'failed'")[0]
    gold = pg.get_first(f"SELECT COUNT(*) FROM {BUILD_SCHEMA}.fct_orders")[0]
    if silver and not gold:
        raise ValueError(f"fct_orders is empty but Silver has {silver:,} orders")


with DAG(
    dag_id="gold_dbt_pipeline",
    description="Daily dbt Gold layer: Silver (Postgres) → Gold dims/facts/aggs",
    schedule=[SILVER_DATASET],
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

    prepare = PythonOperator(task_id="prepare_build_schema", python_callable=prepare_build_schema)

    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=f"{DBT_CMD} seed {DBT_ARGS}")

    dbt_source_test = BashOperator(
        task_id="dbt_source_test",
        bash_command=f"{DBT_CMD} test --select source:silver {DBT_ARGS}",
    )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_CMD} source freshness {DBT_ARGS}",
    )

    audit = PythonOperator(task_id="audit_gold", python_callable=audit_gold)

    # One attempt: a retry after a partial failure would publish nothing new anyway.
    publish = PythonOperator(task_id="publish_gold", python_callable=publish_gold, retries=0)

    (
        dbt_deps
        >> dbt_source_test
        >> dbt_freshness
        >> prepare
        >> dbt_seed
        >> build_layer("staging")
        >> build_layer("dimensions")
        >> build_layer("facts")
        >> build_layer("aggregates")
        >> audit
        >> publish
    )
