"""
DAG: silver_pipeline
====================
Runs the Spark Silver batch job daily at 02:00 UTC.

Uses DockerOperator to spin up a fresh apache/spark container,
run silver_processor.py, then exit. Mounts use absolute host paths
passed via the HOST_PROJECT_DIR environment variable.

Setup required:
  Add to your airflow environment in docker-compose.yml:
    HOST_PROJECT_DIR: ${HOST_PROJECT_DIR}
  Add to your .env file:
    HOST_PROJECT_DIR=/absolute/path/to/project
  On Windows WSL2 example:
    HOST_PROJECT_DIR=/mnt/c/Users/JeremiahAnkuCoblah/Desktop/StreamCart-Real-Time-E-Commerce-Intelligence-Platform
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from docker.types import Mount



PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", "")
print(os.path.join(PROJECT_DIR, "consumer"))

if not PROJECT_DIR:
    raise ValueError(
        "HOST_PROJECT_DIR environment variable is not set. "
        "Add it to your .env file and airflow environment in docker-compose.yml. "
        "Example: HOST_PROJECT_DIR=/mnt/c/Users/yourname/Desktop/StreamCart-..."
    )

def _host(relative_path: str) -> str:
    """Returns absolute host path for a project subfolder."""
    return os.path.join(PROJECT_DIR, relative_path)


# Default args
default_args = {
    "owner":             "streamcart",
    "retries":           2,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}


# Observability helper 
def log_silver_row_counts():
    """Log row counts in silver tables after the Spark job completes."""
    hook = PostgresHook(postgres_conn_id="streamcart_postgres")
    tables = [
        "silver.transactions",
        "silver.clickstream",
        "silver.inventory",
    ]
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


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="silver_pipeline",
    description="Daily Spark Silver batch: Bronze (MinIO) → Silver (Postgres)",
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["streamcart", "silver", "spark"],
) as dag:

    run_silver_spark = DockerOperator(
        task_id="run_silver_spark",
        image="apache/spark:3.5.0-python3",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",

        # Must match the exact network name Docker created
        # Run: docker network ls  — to confirm the name
        network_mode="streamcart-real-time-e-commerce-intelligence-platform_streamcart-net",

        # Pass all required env vars into the Spark container
        environment={
            "PYTHONPATH":          "/opt/airflow",
            "MINIO_ROOT_USER":     os.environ.get("MINIO_ROOT_USER", ""),
            "MINIO_ROOT_PASSWORD": os.environ.get("MINIO_ROOT_PASSWORD", ""),
            "MINIO_BUCKET":        os.environ.get("MINIO_BUCKET", ""),
            "POSTGRES_HOST":       os.environ.get("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT":       os.environ.get("POSTGRES_PORT", "5432"),
            "POSTGRES_USER":       os.environ.get("POSTGRES_USER", ""),
            "POSTGRES_PASSWORD":   os.environ.get("POSTGRES_PASSWORD", ""),
            "POSTGRES_DB":         os.environ.get("POSTGRES_DB", ""),
        },

        # Mount project folders — source must be absolute HOST paths
        mounts=[
            Mount(source=_host("consumer"), target="/opt/airflow/consumer", type="bind"),
            Mount(source=_host("producer"), target="/opt/airflow/producer", type="bind"),
            Mount(source=_host("logs"),     target="/opt/airflow/logs",     type="bind"),
            Mount(source=_host("data"),     target="/opt/airflow/data",     type="bind"),
        ],

        # spark-submit command — identical to spark-silver-job in docker-compose
        command=(
            "/opt/spark/bin/spark-submit "
            "--master local[2] "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,"
                        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                        "org.postgresql:postgresql:42.7.3 "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=${MINIO_ROOT_USER} "
            "--conf spark.hadoop.fs.s3a.secret.key=${MINIO_ROOT_PASSWORD} "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "--conf spark.sql.shuffle.partitions=4 "
            "/opt/airflow/consumer/silver_processor.py"
        ),

        auto_remove="success",
        tty=False,
        do_xcom_push=False,
    )

    check_silver_counts = PythonOperator(
        task_id="check_silver_counts",
        python_callable=log_silver_row_counts,
    )

    run_silver_spark >> check_silver_counts