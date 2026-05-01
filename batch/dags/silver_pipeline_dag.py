"""
DAG: silver_pipeline
====================
Runs the Spark Silver batch job every hour.

Airflow Scheduler → SparkSubmitOperator → spark-master:7077 → Postgres (silver schema)

The actual processing logic lives in consumer/silver_processor.py.
This DAG only submits that script to the running Spark cluster.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "streamcart",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="silver_pipeline",
    default_args=default_args,
    description="Hourly Bronze → Silver batch job via Spark",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["silver", "spark", "batch"],
) as dag:

    run_silver = SparkSubmitOperator(
        task_id="run_silver_processor",
        application="/opt/airflow/consumer/silver_processor.py",
        conn_id="spark_default",
        master="spark://spark-master:7077",

        # Same packages used by the bronze job + postgresql JDBC driver
        packages=(
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.7.3"
        ),

        # MinIO / S3A config
        conf={
            "spark.hadoop.fs.s3a.endpoint":                    "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access":           "true",
            "spark.hadoop.fs.s3a.impl":                        "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider":    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.sql.shuffle.partitions":                     "4",
            "spark.driver.memory":                              "2g",
            "spark.executor.memory":                            "2g",
        },

        # Credentials are injected via environment — Spark picks them up through os.getenv()
        env_vars={
            "MINIO_ROOT_USER":   "{{ var.value.get('MINIO_ROOT_USER', '') }}",
            "MINIO_ROOT_PASSWORD": "{{ var.value.get('MINIO_ROOT_PASSWORD', '') }}",
            "MINIO_BUCKET":      "{{ var.value.get('MINIO_BUCKET', 'streamcart-data') }}",
            "POSTGRES_HOST":     "postgres",
            "POSTGRES_PORT":     "5432",
            "POSTGRES_DB":       "{{ var.value.get('POSTGRES_DB', 'streamcart_db') }}",
            "POSTGRES_USER":     "{{ var.value.get('POSTGRES_USER', 'postgres') }}",
            "POSTGRES_PASSWORD": "{{ var.value.get('POSTGRES_PASSWORD', 'postgres') }}",
        },

        # Python path so silver_processor can import from producer/
        driver_java_options="-DPYTHONPATH=/opt/airflow",

        verbose=True,
        execution_timeout=timedelta(minutes=30),
    )
