"""
DAG: observability_monitor
==========================
Every 15 minutes, samples pipeline health into the observability schema:
  - row counts per table
  - data freshness per table
  - schema changes, in warehouse columns and in Kafka contracts
  - reconciliation: does Silver hold the orders the system of record says exist?
  - volume: did yesterday bring in far less data than usual?
  - fraud rules: precision and recall against the simulator's hidden labels

The Grafana observability dashboard reads these, alongside Airflow's own dag_run table.
"""

import hashlib
import json
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common import assess_volume, record_failure

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


def reconcile_orders():
    """
    Compare each recent complete day in the OLTP database against Silver. A small gap is
    expected, because the simulator corrupts about 1% of events in the stream only.
    """
    pg = hook()
    pg.run("""
        INSERT INTO observability.reconciliation
            (business_date, oltp_orders, silver_orders, missing_pct, checked_at)
        SELECT o.day, o.orders, coalesce(s.orders, 0),
               round(100.0 * greatest(o.orders - coalesce(s.orders, 0), 0) / o.orders, 2), now()
        FROM (SELECT created_at::date AS day, count(*) AS orders FROM oltp.orders
              WHERE created_at::date BETWEEN current_date - 7 AND current_date - 1 GROUP BY 1) o
        LEFT JOIN (SELECT event_time::date AS day, count(*) AS orders FROM silver.transactions
                   GROUP BY 1) s USING (day)
        ON CONFLICT (business_date) DO UPDATE SET
            oltp_orders = EXCLUDED.oltp_orders,
            silver_orders = EXCLUDED.silver_orders,
            missing_pct = EXCLUDED.missing_pct,
            checked_at = EXCLUDED.checked_at
    """)


VOLUME_TABLES = {"silver.transactions": "event_time", "silver.clickstream": "event_time", "silver.inventory": "event_time"}


def check_volumes():
    """
    Compare yesterday's rows in each Silver table with the median of the 7 days before.
    Fails the task, and so records an alert, only the first time a day is flagged, so a
    known drop doesn't raise a new alert every 15 minutes.
    """
    pg = hook()
    newly_flagged = []
    for table, column in VOLUME_TABLES.items():
        counts = dict(pg.get_records(f"""
            SELECT {column}::date, count(*) FROM {table}
            WHERE {column}::date BETWEEN current_date - 8 AND current_date - 1
            GROUP BY 1"""))
        day = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        history = [counts[d] for d in counts if d < day]
        count = counts.get(day, 0)
        baseline, ratio, flagged = assess_volume(count, history)
        already = pg.get_first(
            "SELECT flagged FROM observability.volume_checks WHERE business_date = %s AND table_name = %s",
            parameters=(day, table))
        pg.run("""
            INSERT INTO observability.volume_checks
                (business_date, table_name, row_count, baseline, ratio, flagged, checked_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (business_date, table_name) DO UPDATE SET
                row_count = EXCLUDED.row_count, baseline = EXCLUDED.baseline, ratio = EXCLUDED.ratio,
                flagged = EXCLUDED.flagged, checked_at = EXCLUDED.checked_at""",
            parameters=(day, table, count, baseline, ratio, flagged))
        print(f"{table} {day}: {count:,} rows, baseline {baseline}, ratio {ratio}, flagged {flagged}")
        if flagged and not (already and already[0]):
            newly_flagged.append(f"{table} had {count:,} rows on {day}, against a usual {baseline:,.0f}")
    if newly_flagged:
        raise ValueError("Volume drop: " + "; ".join(newly_flagged))


FRAUD_EVALUATION_SQL = """
WITH window_orders AS (
    -- Recent orders whose alerts have had time to arrive
    SELECT transaction_id, user_id, is_fraud, fraud_type FROM realtime.transactions
    WHERE event_time BETWEEN now() - interval '24 hours' AND now() - interval '10 minutes'
      AND is_fraud IS NOT NULL
),
alerts AS (
    SELECT DISTINCT transaction_id, user_id, fraud_signal FROM realtime.fraud_alerts
    WHERE event_time BETWEEN now() - interval '24 hours' AND now() - interval '10 minutes'
),
-- Takeovers and geo anomalies are judged per order
order_level AS (
    SELECT s.signal,
           count(*) FILTER (WHERE o.fraud_type = s.fraud_type AND a.transaction_id IS NOT NULL) AS tp,
           count(*) FILTER (WHERE (o.fraud_type IS DISTINCT FROM s.fraud_type) AND a.transaction_id IS NOT NULL) AS fp,
           count(*) FILTER (WHERE o.fraud_type = s.fraud_type AND a.transaction_id IS NULL) AS fn
    FROM (VALUES ('HIGH_VALUE_NEW_SESSION', 'account_takeover'), ('GEO_ANOMALY', 'geo_anomaly')) s(signal, fraud_type)
    CROSS JOIN window_orders o
    LEFT JOIN alerts a ON a.transaction_id = o.transaction_id AND a.fraud_signal = s.signal
    GROUP BY s.signal
),
-- Card testing is a pattern across orders, so velocity is judged per user
users AS (
    SELECT user_id, bool_or(fraud_type = 'card_testing') AS card_tester FROM window_orders GROUP BY user_id
),
velocity AS (
    SELECT 'VELOCITY_BREACH'::text AS signal,
           count(*) FILTER (WHERE u.card_tester AND v.user_id IS NOT NULL) AS tp,
           count(*) FILTER (WHERE NOT u.card_tester AND v.user_id IS NOT NULL) AS fp,
           count(*) FILTER (WHERE u.card_tester AND v.user_id IS NULL) AS fn
    FROM users u
    LEFT JOIN (SELECT DISTINCT user_id FROM alerts WHERE fraud_signal = 'VELOCITY_BREACH') v USING (user_id)
)
INSERT INTO observability.fraud_evaluation
    (evaluated_at, fraud_signal, true_positives, false_positives, false_negatives, precision_pct, recall_pct)
SELECT now(), signal, tp, fp, fn,
       round(100.0 * tp / nullif(tp + fp, 0), 1), round(100.0 * tp / nullif(tp + fn, 0), 1)
FROM (SELECT * FROM order_level UNION ALL SELECT * FROM velocity) scored
"""


def evaluate_fraud_rules():
    """Score each real-time fraud rule against the simulator's labels over the last 24 hours."""
    pg = hook()
    pg.run(FRAUD_EVALUATION_SQL)
    for row in pg.get_records("""
        SELECT fraud_signal, true_positives, false_positives, false_negatives, precision_pct, recall_pct
        FROM observability.fraud_evaluation WHERE evaluated_at = (SELECT max(evaluated_at) FROM observability.fraud_evaluation)"""):
        print("%s: tp=%s fp=%s fn=%s precision=%s%% recall=%s%%" % row)


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
    description="Row counts, freshness, schema changes, reconciliation and volume checks",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "streamcart", "on_failure_callback": record_failure, "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["observability"],
) as dag:

    PythonOperator(task_id="sample_counts_and_freshness", python_callable=sample_counts_and_freshness)
    PythonOperator(task_id="detect_schema_changes", python_callable=detect_schema_changes)
    PythonOperator(task_id="reconcile_orders", python_callable=reconcile_orders)
    # No retry: the check is deterministic, and the failure itself is the alert.
    PythonOperator(task_id="check_volumes", python_callable=check_volumes, retries=0)
    PythonOperator(task_id="evaluate_fraud_rules", python_callable=evaluate_fraud_rules)
