"""
DAG: silver_pipeline
====================
Runs the Spark Silver batch job daily at 02:00 UTC.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from docker.types import Mount 

default_args = {
    "owner": "streamcart",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
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
    
    run_silver = DockerOperator(
        task_id="run_silver_spark",
        image="apache/spark:3.5.0-python3",
        api_version="auto",
        auto_remove=True,
        mount_tmp_dir=False,
        user="root",
        command="""
        bash -c "
            # Create Ivy cache directory with proper permissions
            mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && \
            chown -R spark:spark /home/spark/.ivy2 && \
            /opt/spark/bin/spark-submit \
                --master local[2] \
                --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3 \
                --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
                --conf spark.hadoop.fs.s3a.access.key=minioadmin \
                --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
                /opt/airflow/consumer/silver_processor.py
        "
        """,
        docker_url="unix://var/run/docker.sock",
        network_mode="streamcart_streamcart-net",  # Make sure this matches your network
        mounts=[
            # Use absolute paths from the host perspective
            Mount(source="/mnt/c/Users/JeremiahAnkuCoblah/Desktop/StreamCart-Real-Time-E-Commerce-Intelligence-Platform/consumer", target="/opt/airflow/consumer", type="bind"),
            Mount(source="/mnt/c/Users/JeremiahAnkuCoblah/Desktop/StreamCart-Real-Time-E-Commerce-Intelligence-Platform/producer", target="/opt/airflow/producer", type="bind"),
            Mount(source="/mnt/c/Users/JeremiahAnkuCoblah/Desktop/StreamCart-Real-Time-E-Commerce-Intelligence-Platform/logs", target="/opt/airflow/logs", type="bind"),
            Mount(source="/mnt/c/Users/JeremiahAnkuCoblah/Desktop/StreamCart-Real-Time-E-Commerce-Intelligence-Platform/data", target="/opt/airflow/data", type="bind"),
            Mount(source="/mnt/c/Users/JeremiahAnkuCoblah/Desktop/StreamCart-Real-Time-E-Commerce-Intelligence-Platform/jars", target="/opt/airflow/jars", type="bind"),
        ],
        environment={
            "PYTHONPATH": "/opt/airflow",
        },
    )

    check_counts = PythonOperator(
        task_id="check_silver_counts",
        python_callable=log_silver_row_counts,
        provide_context=True,
    )

    run_silver >> check_counts