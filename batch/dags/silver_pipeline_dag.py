"""
DAG: silver_pipeline
====================
Runs the Spark Silver batch job every hour.

Airflow scheduler → SparkSubmitOperator → spark-master:7077 → Postgres (silver schema)

The processing logic lives in consumer/silver_processor.py. This DAG only submits
that script to the running cluster, in client mode: the driver runs inside the
scheduler container and inherits MinIO/Postgres settings from its environment.
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
    max_active_runs=1,
    tags=["silver", "spark", "batch"],
) as dag:

    run_silver = SparkSubmitOperator(
        task_id="run_silver_processor",
        application="/opt/airflow/consumer/silver_processor.py",
        conn_id="spark_default",
        pool="spark_batch",
        conf={
            # Executors connect back to the driver, so it needs a resolvable name.
            # LocalExecutor runs every task in the scheduler container.
            "spark.driver.host": "airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.memory": "1g",
            # The streaming Bronze job holds the rest of the worker's cores.
            "spark.cores.max": "2",
            "spark.executor.memory": "1g",
            "spark.sql.shuffle.partitions": "4",
        },
        verbose=False,
        execution_timeout=timedelta(minutes=30),
    )
