"""Catches DAG import errors, the failure that blocks a pipeline before anything runs."""

import os
from pathlib import Path

import pytest
from airflow.models import DagBag

REPO_ROOT = Path(__file__).resolve().parent.parent
# The containers mount batch/dags at /opt/airflow/dags.
DEFAULT_DAG_FOLDER = REPO_ROOT / "dags" if (REPO_ROOT / "dags").exists() else REPO_ROOT / "batch" / "dags"
DAG_FOLDER = os.getenv("DAG_FOLDER", str(DEFAULT_DAG_FOLDER))

EXPECTED = {
    "silver_pipeline": "@hourly",
    "fx_rates_refresh": "0 * * * *",
    "external_sources": "5 * * * *",
    "oltp_daily_batch": "30 0 * * *",
    "historical_backfill": "0 2 * * 0",
    "bronze_maintenance": "0 4 * * *",
    "observability_monitor": "*/15 * * * *",
}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    # An empty or wrong folder also has no import errors, so check DAGs were found.
    assert set(EXPECTED) | {"gold_dbt_pipeline"} <= set(dagbag.dags)


@pytest.mark.parametrize("dag_id,schedule", EXPECTED.items())
def test_dag_exists_with_schedule(dagbag, dag_id, schedule):
    assert dagbag.dags[dag_id].schedule_interval == schedule


@pytest.mark.parametrize("dag_id", [*EXPECTED, "gold_dbt_pipeline"])
def test_every_dag_records_failures(dagbag, dag_id):
    assert dagbag.dags[dag_id].default_args["on_failure_callback"].__name__ == "record_failure"


def test_gold_runs_when_silver_is_updated(dagbag):
    from common import SILVER_DATASET
    gold = dagbag.dags["gold_dbt_pipeline"]
    assert [d.uri for d in gold.dataset_triggers] == [SILVER_DATASET.uri]
    for dag_id, task_id in [("silver_pipeline", "run_silver_processor"), ("historical_backfill", "reprocess_silver")]:
        outlets = dagbag.dags[dag_id].get_task(task_id).outlets
        assert [d.uri for d in outlets] == [SILVER_DATASET.uri]


def test_gold_runs_layers_in_dependency_order(dagbag):
    dag = dagbag.dags["gold_dbt_pipeline"]
    order = ["dbt_deps", "dbt_seed", "dbt_source_test", "dbt_source_freshness", "dbt_build_staging", "dbt_build_dimensions",
             "dbt_build_facts", "dbt_build_aggregates", "log_gold_counts"]
    for upstream, downstream in zip(order, order[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids
