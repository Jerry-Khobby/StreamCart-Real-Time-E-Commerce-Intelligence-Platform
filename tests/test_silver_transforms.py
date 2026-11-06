"""Silver cleaning rules, run on a local Spark session against small hand-built frames."""

from datetime import datetime, timedelta, timezone

import pytest
from pyspark.sql import SparkSession

from consumer.silver_processor import (
    clean_inventory, clean_transactions, deduplicate, filter_bots, normalise_currency,
    rejected_rows, stitch_sessions, with_event_age, with_user_known,
)

T0 = datetime(2026, 11, 1, 12, 0, 0)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]").appName("silver-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def clicks(spark, rows):
    return spark.createDataFrame(rows, "event_id string, session_id string, user_id string, event_time timestamp, user_agent string")


def test_filter_bots_drops_declared_bot_user_agents(spark):
    df = clicks(spark, [
        ("1", "s1", "u1", T0, "Mozilla/5.0 (compatible; Googlebot/2.1)"),
        ("2", "s2", "u2", T0, "python-requests/2.28"),
        ("3", "s3", "u3", T0, "Mozilla/5.0 (Windows NT 10.0)"),
    ])
    assert [r.event_id for r in filter_bots(df).collect()] == ["3"]


def test_filter_bots_catches_a_spoofed_browser_by_request_rate(spark):
    browser = "Mozilla/5.0 (Windows NT 10.0)"
    rows = [(f"bot{i}", "fast", "u1", T0 + timedelta(seconds=i), browser) for i in range(6)]
    rows += [(f"human{i}", "slow", "u2", T0 + timedelta(seconds=i * 30), browser) for i in range(6)]
    kept = {r.event_id for r in filter_bots(clicks(spark, rows)).collect()}
    assert kept == {f"human{i}" for i in range(6)}


def test_filter_bots_keeps_a_short_session(spark):
    browser = "Mozilla/5.0 (Windows NT 10.0)"
    rows = [("a", "s", "u", T0, browser), ("b", "s", "u", T0 + timedelta(seconds=1), browser)]
    assert filter_bots(clicks(spark, rows)).count() == 2


def stitched_ids(spark, gaps_minutes, user="u1"):
    rows, t = [], T0
    for i, gap in enumerate(gaps_minutes):
        t += timedelta(minutes=gap)
        rows.append((str(i), "client", user, t, "ua"))
    result = stitch_sessions(clicks(spark, rows)).orderBy("event_time").collect()
    return [r.session_id_silver for r in result]


def test_events_within_30_minutes_share_a_session(spark):
    assert len(set(stitched_ids(spark, [0, 10, 29]))) == 1


def test_a_gap_over_30_minutes_starts_a_new_session(spark):
    ids = stitched_ids(spark, [0, 10, 31, 5])
    assert ids[0] == ids[1] and ids[2] == ids[3] and ids[0] != ids[2]


def test_session_id_is_stable_when_later_events_arrive(spark):
    """Reprocessing with more history mustn't rename a session already in Silver."""
    early = stitched_ids(spark, [0, 10])
    later = stitched_ids(spark, [0, 10, 5, 5])
    assert early[0] == later[0]


def test_deduplicate_keeps_the_latest_ingested_copy(spark):
    df = spark.createDataFrame(
        [("t1", "old", T0, T0), ("t1", "new", T0 + timedelta(hours=1), T0), ("t2", "only", T0, T0)],
        "transaction_id string, status string, _ingested_at timestamp, event_time timestamp",
    )
    rows = {r.transaction_id: r.status for r in deduplicate(df, "transaction_id").collect()}
    assert rows == {"t1": "new", "t2": "only"}


def fx_rates(spark):
    return spark.createDataFrame(
        [("EUR", 1.00, T0 - timedelta(hours=2), "api"), ("EUR", 2.00, T0 - timedelta(hours=1), "api"),
         ("EUR", 3.00, T0 + timedelta(hours=1), "api")],
        "currency string, rate_to_usd double, fetched_at timestamp, source string",
    )


def tx_for_fx(spark, rows):
    return spark.createDataFrame(rows, "transaction_id string, currency string, amount_local double, amount_usd double, event_time timestamp")


def test_converts_at_the_rate_in_force_at_transaction_time(spark):
    df = tx_for_fx(spark, [("t1", "EUR", 10.0, 20.0, T0)])
    row = normalise_currency(df, fx_rates(spark)).first()
    assert row.fx_rate_used == 2.0
    assert row.amount_usd_normalised == 20.0
    assert row.fx_discrepancy is False


def test_flags_a_discrepancy_with_the_reported_usd_amount(spark):
    df = tx_for_fx(spark, [("t1", "EUR", 10.0, 99.0, T0)])
    assert normalise_currency(df, fx_rates(spark)).first().fx_discrepancy is True


def test_falls_back_when_no_rate_predates_the_transaction(spark):
    early = T0 - timedelta(days=30)
    df = tx_for_fx(spark, [("t1", "EUR", 10.0, 10.9, early)])
    assert normalise_currency(df, fx_rates(spark)).first().fx_rate_used == 1.09


def test_user_known_flags_unknown_users(spark):
    df = spark.createDataFrame([("u1",), ("ghost",)], "user_id string")
    known = spark.createDataFrame([("u1",)], "user_id string")
    flags = {r.user_id: r.user_known for r in with_user_known(df, known).collect()}
    assert flags == {"u1": True, "ghost": False}


TX_SCHEMA = (
    "transaction_id string, user_id string, product_id string, region string, currency string, amount_local double, "
    "amount_usd double, status string, event_time timestamp, is_backfill boolean, _ingested_at timestamp"
)


@pytest.fixture
def now():
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def run_clean(spark, now, overrides_list):
    base = dict(transaction_id="t", user_id="u1", product_id="p", region="EU", currency="EUR", amount_local=10.0,
                amount_usd=10.9, status="completed", event_time=now, is_backfill=False, _ingested_at=now)
    rows = [{**base, "transaction_id": f"t{i}", **o} for i, o in enumerate(overrides_list)]
    df = spark.createDataFrame(rows, TX_SCHEMA)
    known = spark.createDataFrame([("u1",)], "user_id string")
    fx = spark.createDataFrame([], "currency string, rate_to_usd double, fetched_at timestamp, source string")
    prepared = with_user_known(with_event_age(df, now), known)
    return clean_transactions(prepared, fx)


@pytest.mark.parametrize("fault", [
    {"user_id": None},
    {"user_id": "ghost"},
    {"amount_usd": -5.0},
    {"amount_usd": 999_999.0},
    {"currency": "XXX"},
    {"status": "weird"},
    {"event_time": None},
])
def test_clean_transactions_rejects_each_injected_fault(spark, now, fault):
    assert run_clean(spark, now, [fault]).count() == 0


def test_clean_transactions_rejects_future_events(spark, now):
    assert run_clean(spark, now, [{"event_time": now + timedelta(days=2)}]).count() == 0


def test_clean_transactions_rejects_events_older_than_7_days(spark, now):
    assert run_clean(spark, now, [{"event_time": now - timedelta(days=10)}]).count() == 0


def test_explicit_backfill_is_exempt_from_the_age_limit(spark, now):
    assert run_clean(spark, now, [{"event_time": now - timedelta(days=10), "is_backfill": True}]).count() == 1


def test_clean_transactions_keeps_good_rows(spark, now):
    assert run_clean(spark, now, [{}, {}, {"currency": "XXX"}]).count() == 2


def test_rejected_rows_keeps_bad_rows_but_not_duplicates_of_good_ones(spark):
    raw = spark.createDataFrame(
        [("good", "a"), ("good", "duplicate"), ("bad", "b"), (None, "null key")], "event_id string, note string"
    )
    cleaned = spark.createDataFrame([("good", "a")], "event_id string, note string")
    rows = rejected_rows(raw, cleaned, "event_id", "inventory", T0).collect()
    assert sorted(str(r.record_key) for r in rows) == ["None", "bad"]
    assert all(r.source_table == "inventory" for r in rows)
    assert any('"note":"b"' in r.record for r in rows)


def test_clean_inventory_rejects_zero_delta_and_bad_type(spark, now):
    df = spark.createDataFrame(
        [("e1", "p", "w", "US", "restock", 5, 10, now, False, now),
         ("e2", "p", "w", "US", "restock", 0, 10, now, False, now),
         ("e3", "p", "w", "US", "stolen", 5, 10, now, False, now)],
        "event_id string, product_id string, warehouse_id string, region string, update_type string, "
        "quantity_delta int, stock_after int, event_time timestamp, is_backfill boolean, _ingested_at timestamp",
    )
    kept = {r.event_id for r in clean_inventory(with_event_age(df, now)).collect()}
    assert kept == {"e1"}
