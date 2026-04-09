"""
DAG: silver_pipeline
====================
Runs the Spark Silver batch job daily at 02:00 UTC.

Architecture:
Airflow Scheduler → docker compose run → spark-silver-job → Postgres

No bind path hacks.
No manual container deletion.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    "owner": "streamcart",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def log_silver_row_counts():
    hook = PostgresHook(postgres_conn_id="streamcart_postgres")

    tables = [
        "silver.transactions",
        "silver.clickstream",
        "silver.inventory",
    ]

    print("\n" + "=" * 60)
    print("SILVER ROW COUNTS")
    print("=" * 60)

    for table in tables:
        try:
            count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
            print(f"{table:<35} {count:>10,} rows")
        except Exception as e:
            print(f"{table:<35} ERROR: {e}")

    print("=" * 60)


with DAG(
    dag_id="silver_pipeline",
    description="Bronze (MinIO) → Silver (Postgres)",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["streamcart", "spark", "silver"],
) as dag:

    run_silver = BashOperator(
        task_id="run_silver_spark",
        bash_command="""
    set -e
    echo "Starting Spark Silver job..."

    docker-compose \
        -f /opt/airflow/docker-compose.yml \
        run --rm --no-deps spark-silver-job

    echo "Spark Silver job finished."
    """,
    )

    check_counts = PythonOperator(
        task_id="check_silver_counts",
        python_callable=log_silver_row_counts,
    )

    run_silver >> check_counts