"""
DAG: bronze_maintenance
=======================
Daily 90-day retention (archive to cold storage, then delete) and Iceberg table
maintenance. The logic is in consumer/bronze_maintenance.py.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from common import SPARK_CONF, SPARK_POOL, record_failure

with DAG(
    dag_id="bronze_maintenance",
    description="Bronze retention, compaction and snapshot expiry",
    schedule_interval="0 4 * * *",  # 04:00 UTC, after the Gold build
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "streamcart", "on_failure_callback": record_failure, "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["bronze", "maintenance", "spark"],
) as dag:

    maintain = SparkSubmitOperator(
        task_id="archive_and_compact",
        application="/opt/airflow/consumer/bronze_maintenance.py",
        conn_id="spark_default",
        pool=SPARK_POOL,
        conf=SPARK_CONF,
        execution_timeout=timedelta(hours=1),
    )
