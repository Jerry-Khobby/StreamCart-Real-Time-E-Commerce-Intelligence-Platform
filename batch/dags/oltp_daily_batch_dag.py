"""
DAG: oltp_daily_batch
=====================
Daily pull of yesterday's orders and payments, plus a users snapshot, from the
transactional Postgres database into Bronze.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from common import SPARK_CONF, SPARK_POOL

with DAG(
    dag_id="oltp_daily_batch",
    description="Daily batch extract: Postgres OLTP → Bronze",
    schedule_interval="30 0 * * *",  # 00:30 UTC, once yesterday is complete
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "streamcart", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["bronze", "oltp", "spark", "batch"],
) as dag:

    extract = SparkSubmitOperator(
        task_id="extract_oltp",
        application="/opt/airflow/consumer/oltp_extract.py",
        application_args=["--date", "{{ ds }}"],
        conn_id="spark_default",
        pool=SPARK_POOL,
        conf=SPARK_CONF,
        execution_timeout=timedelta(minutes=30),
    )
