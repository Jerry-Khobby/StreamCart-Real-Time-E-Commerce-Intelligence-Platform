"""Settings shared by the DAGs."""

from datetime import datetime, timezone

from airflow.datasets import Dataset

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

# Updated whenever a Silver job succeeds. Gold is scheduled on it, so it builds from
# fresh Silver data instead of at a fixed time that may come before Silver has finished.
SILVER_DATASET = Dataset("postgres://postgres:5432/streamcart_db/silver")


def record_failure(context):
    """
    Record a task that has used up its retries, for the Grafana failure panel. A real
    deployment would also page someone here. Never raises: a broken alert mustn't hide
    the failure that caused it.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    instance = context["task_instance"]
    try:
        PostgresHook(postgres_conn_id="streamcart_postgres").insert_rows(
            "observability.pipeline_failures",
            [(datetime.now(timezone.utc), instance.dag_id, instance.task_id, instance.run_id,
              str(context.get("exception"))[:500])],
            ["failed_at", "dag_id", "task_id", "run_id", "error"],
        )
    except Exception as exc:
        print(f"Could not record the failure: {exc}")


# A day with under half its usual volume is flagged. Needs a few days of history first.
VOLUME_DROP_RATIO = 0.5
VOLUME_MIN_HISTORY_DAYS = 3


def assess_volume(count: int, history: list) -> tuple:
    """
    Compare a day's row count with the median of the days before it.
    Returns (baseline, ratio, flagged). The median ignores one unusual day in the history.
    Only drops are flagged: a silent loss of data is the failure this exists to catch.
    """
    if len(history) < VOLUME_MIN_HISTORY_DAYS:
        return None, None, False
    ordered = sorted(history)
    middle = len(ordered) // 2
    baseline = ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2
    if not baseline:
        return baseline, None, False
    ratio = count / baseline
    return baseline, round(ratio, 3), ratio < VOLUME_DROP_RATIO
