"""
DAG: silver_pipeline
====================
Runs the Spark Silver batch job daily at 02:00 UTC.

Architecture:
Airflow Scheduler → docker compose run → spark-silver-job → Postgres

No bind path hacks.
No manual container deletion.
"""
#consumer / silver_processor.py 



from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
"owner": "airflow",
"depends_on_past": False,
"retries": 1,
"retry_delay": timedelta(minutes=5),
}

with DAG(
dag_id="silver_pipeline",
description="Run StreamCart Silver Spark batch pipeline",
default_args=default_args,
start_date=datetime(2026, 4, 1),
schedule="0 2 * * *",   # Daily at 02:00 UTC
catchup=False,
max_active_runs=1,
tags=["streamcart", "spark", "silver"],
) as dag:

   run_silver_spark = SparkSubmitOperator(
    task_id="run_silver_spark",

    # Script location inside container
    application="/opt/airflow/consumer/silver_processor.py",

    # Spark cluster
    conn_id="spark_default",

    # Spark master
    conf={
        "spark.master": "spark://spark-master:7077",

        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ROOT_USER }}",
        "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_ROOT_PASSWORD }}",
        "spark.hadoop.fs.s3a.path.style.access": "true",

        "spark.sql.shuffle.partitions": "4",
    },

    # Jars from your jars folder
    jars="/opt/airflow/jars/postgresql-42.7.6.jar",

    # Spark packages required
    packages="org.apache.hadoop:hadoop-aws:3.3.4,"
             "com.amazonaws:aws-java-sdk-bundle:1.12.262",

    executor_memory="2g",
    driver_memory="2g",

    verbose=True,
)


run_silver_spark

