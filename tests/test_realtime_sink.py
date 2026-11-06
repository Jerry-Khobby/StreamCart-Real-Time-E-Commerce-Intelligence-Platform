"""What the serving sink lets through to the live dashboards."""

from datetime import datetime, timedelta, timezone

import pytest

from consumer.realtime_sink import to_row


def transaction(**overrides):
    event = {
        "transaction_id": "t1", "user_id": "USER-0000001", "region": "EU", "amount_usd": 120.0,
        "status": "completed", "timestamp": datetime.now(timezone.utc).isoformat(), "is_backfill": False,
    }
    return {**event, **overrides}


def test_a_good_order_is_served():
    row, _ = to_row("transactions", transaction())
    assert row[0] == "t1" and row[3] == 120.0


@pytest.mark.parametrize("fault", [
    {"amount_usd": 999_999.0},
    {"amount_usd": -5.0},
    {"amount_usd": None},
    {"user_id": None},
    {"timestamp": None},
    {"is_backfill": True},
])
def test_faulty_or_seed_orders_are_not_served(fault):
    assert to_row("transactions", transaction(**fault)) is None


@pytest.mark.parametrize("offset", [timedelta(days=2), timedelta(days=-10)])
def test_orders_outside_the_time_range_are_not_served(offset):
    when = (datetime.now(timezone.utc) + offset).isoformat()
    assert to_row("transactions", transaction(timestamp=when)) is None


def test_ksqldb_alerts_use_upper_case_fields():
    alert = {
        "TRANSACTION_ID": "t1", "FRAUD_SIGNAL": "GEO_ANOMALY", "USER_ID": "u", "AMOUNT_USD": 5.0, "REGION": "EU",
        "EVENT_TIME": datetime.now(timezone.utc).isoformat(),
    }
    row, _ = to_row("fraud_alerts", alert)
    assert row[:2] == ("t1", "GEO_ANOMALY")
