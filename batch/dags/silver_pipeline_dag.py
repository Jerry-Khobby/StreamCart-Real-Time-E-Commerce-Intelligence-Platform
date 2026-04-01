"""
DAG: silver_pipeline
====================
Runs the Spark Silver batch job daily at 02:00 UTC.
 
What it does:
  1. Triggers the Spark silver job (reads Bronze from MinIO, cleans,
     writes to Postgres silver schema)
  2. Waits for it to complete successfully
  3. Logs row counts for observability
 
The Bronze Spark streaming job runs separately 24/7 and is NOT
managed by this DAG — it accumulates data in MinIO continuously.
This DAG just processes whatever has built up since the last run.
"""




from datetime import datetime, timedelta
import os 

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount 
BASE_DIR = os.path.abspath("../../")  


default_args = {
    "owner": "streamcart",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

#postgres validation step 
def log_silver_row_counts():
    """Check row counts in silver tables after the Spark job completes."""
    hook = PostgresHook(postgres_conn_id="streamcart_postgres")

    tables = [
        "silver.transactions",
        "silver.clickstream",
        "silver.inventory",
    ]

    for table in tables:
        count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
        print(f"[silver] {table}: {count:,} rows")
        
        
        

with DAG(
    dag_id="silver_pipeline",
    description="Daily Spark Silver batch job: Bronze → Silver",
    schedule_interval="0 2 * * *",  # 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["streamcart", "silver", "spark"],
) as dag:


    # --------------------------------------------------------------
    # Run Spark Silver job
    # --------------------------------------------------------------
    run_silver_spark = DockerOperator(
    task_id="run_silver_spark",
    image="apache/spark:3.5.0-python3",
    command="""
/opt/spark/bin/spark-submit
--master local[2]
--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3
--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000
--conf spark.hadoop.fs.s3a.path.style.access=true
--conf spark.sql.shuffle.partitions=4
/opt/airflow/consumer/silver_processor.py
""",
    network_mode="streamcart-net",
    mounts=[
        Mount(source=os.path.join(BASE_DIR, "consumer"), target="/opt/airflow/consumer", type="bind"),
        Mount(source=os.path.join(BASE_DIR, "producer"), target="/opt/airflow/producer", type="bind"),
        Mount(source=os.path.join(BASE_DIR, "data"), target="/opt/airflow/data", type="bind"),
        Mount(source=os.path.join(BASE_DIR, "logs"), target="/opt/airflow/logs", type="bind"),
    ],
    auto_remove=True,
    tty=True,
    docker_url="unix://var/run/docker.sock",
)


   
    # Verify data written to Postgres
    check_silver_counts = PythonOperator(
        task_id="check_silver_counts",
        python_callable=log_silver_row_counts,
    )


    run_silver_spark >> check_silver_counts
  