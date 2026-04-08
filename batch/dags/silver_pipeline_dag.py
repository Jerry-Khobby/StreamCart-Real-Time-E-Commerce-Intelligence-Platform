"""
DAG: silver_pipeline
====================
Runs the Spark Silver batch job daily at 02:00 UTC.

Uses BashOperator + docker-compose run. No DockerOperator.
No /mnt/c/ paths. No bind mount issues.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner":             "streamcart",
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def log_silver_row_counts(**context):
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

            echo "Cleaning up any leftover container..."
            docker rm -f streamcart-spark-silver 2>/dev/null || true

            echo "COMPOSE_PROJECT_DIR=$COMPOSE_PROJECT_DIR"
            echo "Running spark-silver-job via docker-compose..."

            docker-compose \
                --project-directory "$COMPOSE_PROJECT_DIR" \
                --file /opt/streamcart/docker-compose.yml \
                --env-file /opt/streamcart/.env \
                run --rm --no-deps \
                spark-silver-job

            echo "Spark silver job completed successfully."
        """,
        env={
            "COMPOSE_PROJECT_DIR":            os.environ.get("COMPOSE_PROJECT_DIR", ""),
            "MINIO_ROOT_USER":                os.environ.get("MINIO_ROOT_USER", ""),
            "MINIO_ROOT_PASSWORD":            os.environ.get("MINIO_ROOT_PASSWORD", ""),
            "MINIO_BUCKET":                   os.environ.get("MINIO_BUCKET", ""),
            "POSTGRES_USER":                  os.environ.get("POSTGRES_USER", ""),
            "POSTGRES_PASSWORD":              os.environ.get("POSTGRES_PASSWORD", ""),
            "POSTGRES_DB":                    os.environ.get("POSTGRES_DB", ""),
            "AIRFLOW__CORE__FERNET_KEY":      os.environ.get("AIRFLOW__CORE__FERNET_KEY", ""),
            "AIRFLOW__WEBSERVER__SECRET_KEY": os.environ.get("AIRFLOW__WEBSERVER__SECRET_KEY", ""),
        },
    )

    check_counts = PythonOperator(
        task_id="check_silver_counts",
        python_callable=log_silver_row_counts,
        provide_context=True,
    )

    run_silver >> check_counts