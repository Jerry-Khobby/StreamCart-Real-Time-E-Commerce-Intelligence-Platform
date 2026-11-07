"""Simulator rules that the rest of the pipeline relies on."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fastavro
import pytest

from producer.config import FX_TO_USD, REGION_PARTITIONS, USER_HOME_REGION, WAREHOUSES
from producer.data_generator import (
    StockBook, corrupt_transaction, demand_weight, fulfil_order, generate_fraud_scenario,
    generate_inventory_update, generate_session, generate_transaction,
)

SCHEMAS = Path(__file__).resolve().parent.parent / "schemas" / "avro"


def contract(topic):
    return fastavro.parse_schema(json.loads((SCHEMAS / f"{topic}-value.avsc").read_text()))


def test_amount_local_converts_back_to_usd():
    for _ in range(200):
        tx = generate_transaction()
        back = tx["amount_local"] * FX_TO_USD[tx["currency"]]
        assert back == pytest.approx(tx["amount_usd"], rel=0.02, abs=0.02)


def test_customers_keep_a_stable_home_market():
    tx = generate_transaction()
    assert tx["region"] == USER_HOME_REGION[tx["user_id"]]
    assert tx["ip_region"] == tx["region"]
    assert tx["region"] in REGION_PARTITIONS


def test_weekends_dip():
    wednesday, saturday = datetime(2026, 11, 4, 11), datetime(2026, 11, 7, 11)
    assert demand_weight(saturday) < demand_weight(wednesday)


def test_morning_peak_beats_the_small_hours():
    assert demand_weight(datetime(2026, 11, 4, 11)) > demand_weight(datetime(2026, 11, 4, 4))


def test_detectable_fraud_trips_its_real_time_rule():
    card = generate_fraud_scenario(scenario="card_testing", evasive=False)
    assert len(card) > 10 and len({e["user_id"] for e in card}) == 1
    takeover = generate_fraud_scenario(scenario="account_takeover", evasive=False)[0]
    assert takeover["amount_usd"] > 5000 and takeover["session_age_seconds"] < 60
    geo = generate_fraud_scenario(scenario="geo_anomaly", evasive=False)[0]
    assert geo["ip_region"] != geo["region"]


def test_evasive_fraud_stays_under_every_threshold():
    card = generate_fraud_scenario(scenario="card_testing", evasive=True)
    assert len(card) <= 10
    assert generate_fraud_scenario(scenario="account_takeover", evasive=True)[0]["amount_usd"] < 5000
    geo = generate_fraud_scenario(scenario="geo_anomaly", evasive=True)[0]
    assert geo["ip_region"] == geo["region"]
    assert all(e["_is_fraud"] for e in card)


def test_some_innocent_orders_look_suspicious():
    import producer.data_generator as g
    orders = [g.generate_customer_order() for _ in range(20000)]
    travellers = [o for o in orders if o["ip_region"] != o["region"]]
    big_and_quick = [o for o in orders if o["amount_usd"] > 5000 and o["session_age_seconds"] < 60]
    assert travellers and big_and_quick
    assert not any(o["_is_fraud"] for o in travellers + big_and_quick)


def test_sessions_never_contain_future_events():
    now = datetime.now(timezone.utc)
    for _ in range(100):
        times = [datetime.fromisoformat(e["timestamp"]) for e in generate_session(now)]
        assert max(times) <= now
        assert times == sorted(times)


def test_bots_carry_no_label():
    assert "_is_bot" not in generate_session()[0]


def test_stock_never_goes_negative():
    book = StockBook()
    assert book.apply("P", "W", -1_000_000) == 0


def test_failed_orders_ship_nothing():
    tx = generate_transaction()
    tx["status"] = "failed"
    assert fulfil_order(StockBook(), tx, datetime.now(timezone.utc)) is None


def test_orders_ship_from_a_warehouse_in_their_market():
    tx = generate_transaction()
    tx["status"] = "completed"
    update = fulfil_order(StockBook(), tx, datetime.now(timezone.utc))
    assert WAREHOUSES[update["warehouse_id"]] == tx["region"]
    assert update["quantity_delta"] == -tx["quantity"]


def test_restock_targets_low_stock_first():
    book = StockBook()
    book.apply("P-LOW", "WH-US-EAST", -1_000_000)
    update = generate_inventory_update(book, update_type="restock")
    assert (update["product_id"], update["warehouse_id"]) == ("P-LOW", "WH-US-EAST")
    assert update["stock_after"] >= 50


@pytest.mark.parametrize("make,topic", [
    (generate_transaction, "transactions"),
    (lambda: generate_session()[0], "clickstream"),
    (lambda: generate_inventory_update(StockBook()), "inventory_updates"),
])
def test_events_satisfy_their_avro_contract(make, topic):
    assert fastavro.validate(make(), contract(topic), strict=True)


def test_corrupted_events_still_satisfy_the_contract():
    """Faults must reach Silver to be counted, so the contract can't reject them."""
    schema = contract("transactions")
    for _ in range(100):
        assert fastavro.validate(corrupt_transaction(generate_transaction()), schema, strict=True)


def test_contract_rejects_a_wrongly_typed_field():
    event = generate_transaction()
    event["quantity"] = "three"
    with pytest.raises(fastavro.validation.ValidationError):
        fastavro.validate(event, contract("transactions"))


def test_backfill_flag_is_carried():
    old = datetime.now(timezone.utc) - timedelta(days=30)
    assert generate_transaction(old, is_backfill=True)["is_backfill"] is True
    assert all(e["is_backfill"] for e in generate_session(old, is_backfill=True))


def test_default_rate_clears_50000_orders_on_every_day_of_the_week():
    from datetime import datetime, timedelta, timezone

    import producer.data_generator as g
    monday = datetime(2026, 11, 2, tzinfo=timezone.utc)
    for day in range(7):
        start = monday + timedelta(days=day)
        orders = sum(g.DEFAULT_TX_RATE * g.demand_weight(start + timedelta(minutes=m)) * 60 for m in range(1440))
        assert orders > 50_000, start.strftime("%A")


def test_pacing_catches_up_after_a_slow_send_but_caps_the_backlog():
    from producer.data_generator import MAX_BACKLOG_SECONDS, next_deadline
    assert next_deadline(100.0, 1.0, now=100.5) == 101.0      # on schedule: wait until due
    assert next_deadline(100.0, 1.0, now=105.0) == 101.0      # behind: due already, send at once
    assert next_deadline(0.0, 1.0, now=1000.0) == 1000.0 - MAX_BACKLOG_SECONDS


def test_rain_makes_a_market_busier_without_taking_orders_from_others():
    import producer.data_generator as g
    lift = g.rain_lift({"US": 0.0, "EU": 3.2, "Asia": None})
    assert lift == {"US": 1.0, "EU": g.RAIN_DEMAND_LIFT, "Asia": 1.0}
    assert g.overall_lift(lift) > 1.0 and g.overall_lift({}) == 1.0

    import random
    random.seed(7)
    dry = sum(g.USER_HOME_REGION[g.pick_customer({})] == "EU" for _ in range(20000))
    wet = sum(g.USER_HOME_REGION[g.pick_customer({"EU": 3.0})] == "EU" for _ in range(20000))
    assert wet > dry * 1.5
