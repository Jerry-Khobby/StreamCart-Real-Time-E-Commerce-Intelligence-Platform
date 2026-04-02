"""
DAG: silver_pipeline
====================
Runs the Spark Silver batch job daily at 02:00 UTC.

Uses BashOperator + `docker compose run spark-silver-job`.
Reuses docker-compose.yml config exactly — same image, mounts,
env vars, and network. No PySpark in Airflow. No path issues.

One-time setup needed in your Dockerfile:
  Install docker CLI inside the Airflow image (see below).
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner":             "streamcart",
    "retries":           2,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}


def log_silver_row_counts(**context):
    """Log row counts after silver job completes."""
    hook = PostgresHook(postgres_conn_id="streamcart_postgres")
    tables = ["silver.transactions", "silver.clickstream", "silver.inventory"]
    print("\n" + "=" * 50)
    print("SILVER ROW COUNTS")
    print("=" * 50)
    for table in tables:
        try:
            count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
            print(f"  {table:<35} {count:>10,} rows")
        except Exception as e:
            print(f"  {table:<35} ERROR: {e}")
    print("=" * 50)


with DAG(
    dag_id="silver_pipeline",
    description="Daily Silver batch: Bronze (MinIO) → Silver (Postgres)",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["streamcart", "silver", "spark"],
) as dag:

    run_silver = BashOperator(
        task_id="run_silver_spark",
        bash_command="""
            set -e

            # Remove leftover container from previous run if any
            docker rm -f streamcart-spark-silver 2>/dev/null || true

            # Run spark-silver-job using the compose file on the host.
            # docker compose reads the file from the host filesystem,
            # so we point it at the host project directory.
            docker-compose \
                -f /opt/streamcart/docker-compose.yml \
                run --rm --no-deps \
                spark-silver-job

            RESULT=$?
            if [ $RESULT -ne 0 ]; then
                echo "ERROR: Spark silver job failed with exit code $RESULT"
                exit $RESULT
            fi

            echo "Spark silver job completed successfully."
        """,
        env={
            "COMPOSE_PROJECT_DIR": os.environ.get("COMPOSE_PROJECT_DIR", ""),
        },
    )

    check_counts = PythonOperator(
        task_id="check_silver_counts",
        python_callable=log_silver_row_counts,
        provide_context=True,
    )

    run_silver >> check_counts