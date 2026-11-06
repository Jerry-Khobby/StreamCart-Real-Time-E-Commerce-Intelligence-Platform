"""Settings shared by the DAGs that submit Spark batch jobs."""

SPARK_CONF = {
    # Executors connect back to the driver, so it needs a resolvable name.
    # LocalExecutor runs every task in the scheduler container.
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.memory": "768m",
    # The streaming Bronze job holds the other one of the worker's three cores.
    "spark.cores.max": "2",
    "spark.executor.memory": "1g",
    "spark.sql.shuffle.partitions": "4",
    "spark.ui.enabled": "false",
}

# Allows one batch job at a time on the single worker (created in airflow-init).
SPARK_POOL = "spark_batch"
