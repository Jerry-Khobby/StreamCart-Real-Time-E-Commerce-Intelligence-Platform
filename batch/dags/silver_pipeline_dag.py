"""
DAG: silver_pipeline
====================
Runs the Spark Silver batch job every hour.

Airflow scheduler → SparkSubmitOperator → spark-master:7077 → Iceberg + Postgres (silver)

The processing logic lives in consumer/silver_processor.py. This DAG only submits
that script to the running cluster, in client mode: the driver runs inside the
scheduler container and inherits MinIO/Postgres settings from its environment.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from common import SILVER_DATASET, SPARK_CONF, SPARK_POOL, record_failure

default_args = {
    "owner": "streamcart",
    "on_failure_callback": record_failure,
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
        pool=SPARK_POOL,
        conf=SPARK_CONF,
        # Marks Silver as updated, which triggers gold_dbt_pipeline.
        outlets=[SILVER_DATASET],
        execution_timeout=timedelta(minutes=45),
    )
