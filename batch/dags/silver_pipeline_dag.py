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
from pyspark.sql import SparkSession 
from consumer.silver_processor import (
    create_spark_session,
    read_bronze_incremental,
    process_transactions,
    process_clickstream,
    process_inventory,
    write_silver,
    load_watermark,
    save_watermark
)

import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from consumer.silver_processor import run_silver_pipeline


def run_silver_pipeline():
    spark = create_spark_session()
    bucket = os.getenv("MINIO_BUCKET")

    # Transactions
    transactions_wm = load_watermark(spark, bucket, "transactions")
    df_trans = read_bronze_incremental(spark, bucket, "transactions", transactions_wm)
    df_trans, metrics_trans = process_transactions(df_trans, spark)
    write_silver(df_trans, spark, "transactions", "transaction_id")
    save_watermark(spark, bucket, "transactions", df_trans.agg({"event_time": "max"}).collect()[0][0])

    # Repeat for clickstream
    click_wm = load_watermark(spark, bucket, "clickstream")
    df_click = read_bronze_incremental(spark, bucket, "clickstream", click_wm)
    df_click, metrics_click = process_clickstream(df_click, spark)
    write_silver(df_click, spark, "clickstream", "event_id")
    save_watermark(spark, bucket, "clickstream", df_click.agg({"event_time": "max"}).collect()[0][0])

    # Repeat for inventory
    inventory_wm = load_watermark(spark, bucket, "inventory")
    df_inventory = read_bronze_incremental(spark, bucket, "inventory", inventory_wm)
    df_inventory, metrics_inv = process_inventory(df_inventory, spark)
    write_silver(df_inventory, spark, "inventory", "event_id")
    save_watermark(spark, bucket, "inventory", df_inventory.agg({"event_time": "max"}).collect()[0][0])

    spark.stop()
    return {
        "transactions": metrics_trans,
        "clickstream": metrics_click,
        "inventory": metrics_inv
    }
    
    

# batch/dags/silver_pipeline_dag.py


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='silver_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval='0 2 * * *',  # daily at 02:00 UTC
    catchup=False,
    max_active_runs=1
) as dag:

    run_silver_task = PythonOperator(
        task_id='run_silver_pipeline',
        python_callable=run_silver_pipeline
    )

    run_silver_task