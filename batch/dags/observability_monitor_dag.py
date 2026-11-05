"""
DAG: observability_monitor
==========================
Every 15 minutes, samples pipeline health into the observability schema:
  - row counts per table
  - data freshness per table
  - schema changes, in warehouse columns and in Kafka contracts

The Grafana observability dashboard reads these, alongside Airflow's own dag_run table.
"""

import hashlib
import json
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

SCHEMA_REGISTRY = "http://schema-registry:8081"

# Table → column holding the time of its newest data
MONITORED_TABLES = {
    "silver.transactions": "event_time",
    "silver.clickstream": "event_time",
    "silver.inventory": "event_time",
    "silver.fx_rates": "fetched_at",
    "silver.weather_hourly": "observed_at",
    "gold.fct_orders": "dbt_updated_at",
    "gold.fct_sessions": "dbt_updated_at",
    "gold.fct_inventory_snapshots": "dbt_updated_at",
    "gold.agg_daily_revenue": "dbt_updated_at",
    "gold.agg_fraud_summary": "dbt_updated_at",
    "gold.agg_inventory_health": "dbt_updated_at",
    "gold.agg_funnel_conversion": "dbt_updated_at",
    "realtime.transactions": "event_time",
    "realtime.fraud_alerts": "event_time",
    "oltp.orders": "created_at",
}
SCHEMAS_TO_WATCH = ("oltp", "silver", "gold", "realtime")


def hook():
    return PostgresHook(postgres_conn_id="streamcart_postgres")


def existing_tables(pg) -> set:
    rows = pg.get_records("""
        SELECT table_schema || '.' || table_name FROM information_schema.tables
        WHERE table_schema IN %s""", parameters=(SCHEMAS_TO_WATCH,))
    return {row[0] for row in rows}


def sample_counts_and_freshness():
    pg = hook()
    sampled_at = datetime.now(timezone.utc)
    tables = existing_tables(pg)
    counts, freshness = [], []

    for table, column in MONITORED_TABLES.items():
        # gold.fct_* have no dbt_updated_at column of their own: fall back to occurred data.
        if table not in tables:
            continue
        columns = {
            row[0] for row in pg.get_records(
                "SELECT column_name FROM information_schema.columns WHERE table_schema || '.' || table_name = %s",
                parameters=(table,),
            )
        }
        time_column = column if column in columns else next(
            (c for c in ("occurred_at", "session_end_at", "snapshot_date") if c in columns), None
        )
        select_time = f"max({time_column})::timestamptz" if time_column else "NULL::timestamptz"
        row_count, latest = pg.get_first(f"SELECT count(*), {select_time} FROM {table}")
        counts.append((sampled_at, table, row_count))
        age = (sampled_at - latest).total_seconds() if latest else None
        freshness.append((sampled_at, table, latest, age))

    pg.insert_rows("observability.row_counts", counts, ["sampled_at", "table_name", "row_count"])
    pg.insert_rows("observability.freshness", freshness, ["sampled_at", "table_name", "latest_at", "age_seconds"])
    print(f"Sampled {len(counts)} tables")


def current_definitions(pg) -> dict:
    """Every watched table's columns and every Kafka contract, as comparable text."""
    definitions = {}
    for table, column, data_type in pg.get_records("""
        SELECT table_schema || '.' || table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema IN %s AND table_name NOT LIKE '%%\\_staging'
        ORDER BY 1, ordinal_position""", parameters=(SCHEMAS_TO_WATCH,)):
        definitions.setdefault(table, []).append(f"{column} {data_type}")
    definitions = {table: ", ".join(cols) for table, cols in definitions.items()}

    subjects = requests.get(f"{SCHEMA_REGISTRY}/subjects", timeout=10)
    subjects.raise_for_status()
    for subject in subjects.json():
        latest = requests.get(f"{SCHEMA_REGISTRY}/subjects/{subject}/versions/latest", timeout=10)
        latest.raise_for_status()
        fields = json.loads(latest.json()["schema"]).get("fields", [])
        names = ", ".join(field["name"] for field in fields)
        definitions[f"kafka:{subject}"] = f"v{latest.json()['version']}: {names}"
    return definitions


def describe_change(old: str, new: str) -> str:
    old_items, new_items = set(old.split(", ")), set(new.split(", "))
    parts = []
    if new_items - old_items:
        parts.append("added: " + ", ".join(sorted(new_items - old_items)))
    if old_items - new_items:
        parts.append("removed: " + ", ".join(sorted(old_items - new_items)))
    return "; ".join(parts) or "reordered"


def detect_schema_changes():
    pg = hook()
    now = datetime.now(timezone.utc)
    known = {name: definition for name, definition in pg.get_records(
        "SELECT object_name, definition FROM observability.schema_state")}
    current = current_definitions(pg)
    first_run = not known
    changes = []

    for name, definition in current.items():
        if name not in known:
            # Objects found on the first scan are the baseline, not changes.
            if not first_run:
                changes.append((now, name, "created", definition))
        elif known[name] != definition:
            changes.append((now, name, "changed", describe_change(known[name], definition)))
    for name in known.keys() - current.keys():
        changes.append((now, name, "dropped", known[name]))

    if changes:
        pg.insert_rows("observability.schema_changes", changes,
                       ["detected_at", "object_name", "change_kind", "detail"])
    for _, name, kind, detail in changes:
        print(f"SCHEMA CHANGE {kind}: {name}: {detail}")

    state = [
        (name, hashlib.sha256(definition.encode()).hexdigest(), definition, now)
        for name, definition in current.items()
    ]
    pg.run("DELETE FROM observability.schema_state")
    pg.insert_rows("observability.schema_state", state, ["object_name", "fingerprint", "definition", "updated_at"])
    print(f"Tracked {len(state)} objects, found {len(changes)} changes")


with DAG(
    dag_id="observability_monitor",
    description="Row counts, freshness and schema change detection",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "streamcart", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["observability"],
) as dag:

    PythonOperator(task_id="sample_counts_and_freshness", python_callable=sample_counts_and_freshness)
    PythonOperator(task_id="detect_schema_changes", python_callable=detect_schema_changes)
