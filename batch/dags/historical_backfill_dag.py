"""
DAG: historical_backfill
========================
Weekly reprocessing. Replays the last 7 days of Bronze through Silver, so rule fixes
and late corrections reach the warehouse. MERGE INTO makes the replay idempotent.
Updating Silver triggers a Gold rebuild through the Silver dataset.

Trigger manually with {"lookback_days": 30} to replay a longer window.

Paused on a fresh install, because Airflow would otherwise run it straight away and
repeat the work the first Silver run has just done. Unpause it to run weekly.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from common import SILVER_DATASET, SPARK_CONF, SPARK_POOL, record_failure

with DAG(
    dag_id="historical_backfill",
    description="Weekly Silver reprocessing of recent Bronze",
    schedule_interval="0 2 * * 0",  # Sundays 02:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    params={"lookback_days": 7},
    default_args={"owner": "streamcart", "on_failure_callback": record_failure, "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["backfill", "spark", "batch"],
) as dag:

    reprocess = SparkSubmitOperator(
        task_id="reprocess_silver",
        application="/opt/airflow/consumer/silver_processor.py",
        application_args=["--lookback-days", "{{ params.lookback_days }}"],
        conn_id="spark_default",
        pool=SPARK_POOL,
        conf=SPARK_CONF,
        execution_timeout=timedelta(hours=2),
        outlets=[SILVER_DATASET],
    )
