"""
DAG: housekeeping
=================
Daily clean-up of data that otherwise grows without limit:

  - observability samples older than 30 days (row counts and freshness are taken every
    15 minutes). Failures, reconciliation, volume checks and schema changes are kept for
    a year, because they're the audit trail.
  - Airflow's own run history and task logs older than 30 days.

The dead-letter table is pruned by bronze_maintenance, alongside the lakehouse tables.
Business data (OLTP, Silver, Gold) is kept: it's the history the reports are built from.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common import record_failure

KEEP_SAMPLES_DAYS = 30
KEEP_AUDIT_DAYS = 365
KEEP_AIRFLOW_DAYS = 30

PRUNE = {
    "observability.row_counts": ("sampled_at", KEEP_SAMPLES_DAYS),
    "observability.freshness": ("sampled_at", KEEP_SAMPLES_DAYS),
    "observability.quality_results": ("run_at", KEEP_SAMPLES_DAYS),
    "observability.silver_runs": ("run_at", KEEP_AUDIT_DAYS),
    "observability.pipeline_failures": ("failed_at", KEEP_AUDIT_DAYS),
    "observability.schema_changes": ("detected_at", KEEP_AUDIT_DAYS),
    "observability.reconciliation": ("business_date", KEEP_AUDIT_DAYS),
    "observability.volume_checks": ("business_date", KEEP_AUDIT_DAYS),
}


def prune_observability():
    pg = PostgresHook(postgres_conn_id="streamcart_postgres")
    for table, (column, days) in PRUNE.items():
        deleted = pg.get_first(f"""
            WITH gone AS (DELETE FROM {table} WHERE {column} < now() - interval '{days} days' RETURNING 1)
            SELECT count(*) FROM gone""")[0]
        print(f"{table}: removed {deleted:,} rows older than {days} days")


with DAG(
    dag_id="housekeeping",
    description="Prune old observability samples, Airflow history and logs",
    schedule_interval="30 4 * * *",  # 04:30 UTC, after bronze_maintenance
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "streamcart", "on_failure_callback": record_failure, "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["maintenance"],
) as dag:

    PythonOperator(task_id="prune_observability", python_callable=prune_observability)

    BashOperator(
        task_id="prune_airflow_history",
        bash_command=(
            "airflow db clean --yes --skip-archive "
            f"--clean-before-timestamp \"$(date -u -d '-{KEEP_AIRFLOW_DAYS} days' +%Y-%m-%dT%H:%M:%S)\""
        ),
    )

    BashOperator(
        task_id="prune_task_logs",
        bash_command=(
            f"find /opt/airflow/logs -type f -name '*.log' -mtime +{KEEP_AIRFLOW_DAYS} -delete && "
            "find /opt/airflow/logs -mindepth 1 -type d -empty -delete && echo done"
        ),
    )
