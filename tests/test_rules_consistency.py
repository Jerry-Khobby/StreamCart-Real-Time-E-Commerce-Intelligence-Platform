"""
Rules that can't import quality/rules.py (SQL, YAML, JSON) must still agree with it.
A rule changed in one place but not the others fails here.
"""

import json
import re
from pathlib import Path

import yaml

from quality import rules

ROOT = Path(__file__).resolve().parent.parent
if not (ROOT / "streaming").exists():  # inside the containers, the repo is split across mounts
    ROOT = Path("/opt/airflow")


def suite(table):
    return json.loads((ROOT / "quality" / "expectations" / f"{table}.json").read_text())


def expectation(table, kind, column):
    return next(e["kwargs"] for e in suite(table) if e["expectation_type"] == kind and e["kwargs"].get("column") == column)


def test_expectation_suites_use_the_shared_value_sets():
    in_set = "expect_column_values_to_be_in_set"
    assert expectation("transactions", in_set, "currency")["value_set"] == rules.MARKET_CURRENCIES
    assert expectation("transactions", in_set, "status")["value_set"] == rules.ORDER_STATUSES
    assert expectation("inventory", in_set, "update_type")["value_set"] == rules.INVENTORY_UPDATE_TYPES
    assert expectation("clickstream", in_set, "page_type")["value_set"] == rules.PAGE_TYPES


def test_expectation_suites_use_the_shared_limits():
    between = "expect_column_values_to_be_between"
    assert expectation("transactions", between, "amount_usd")["max_value"] == rules.MAX_AMOUNT_USD
    for table in ["transactions", "clickstream", "inventory"]:
        age = expectation(table, between, "event_age_seconds")
        assert age["min_value"] == -rules.FUTURE_TOLERANCE_SECONDS
        assert age["max_value"] == rules.MAX_EVENT_AGE_SECONDS


def test_dbt_uses_the_shared_fraud_thresholds():
    variables = yaml.safe_load((ROOT / "batch" / "dbt" / "dbt_project.yml").read_text())["vars"]
    assert variables == {
        "high_value_usd": rules.HIGH_VALUE_USD,
        "new_session_seconds": rules.NEW_SESSION_SECONDS,
        "velocity_max_orders_per_hour": rules.VELOCITY_MAX_ORDERS_PER_HOUR,
        "card_test_max_amount_usd": rules.CARD_TEST_MAX_AMOUNT_USD,
        "card_test_min_small_orders": rules.CARD_TEST_MIN_SMALL_ORDERS,
    }


def test_dbt_source_tests_accept_the_shared_currencies_and_statuses():
    source = yaml.safe_load((ROOT / "batch" / "dbt" / "models" / "staging" / "sources.yml").read_text())
    columns = {c["name"]: c for c in source["sources"][0]["tables"][0]["columns"]}
    def accepted(column):
        return next(t["accepted_values"]["values"] for t in columns[column]["tests"] if isinstance(t, dict))
    assert accepted("currency") == rules.MARKET_CURRENCIES
    assert sorted(accepted("status")) == sorted(rules.ORDER_STATUSES)


def test_ksqldb_uses_the_shared_fraud_thresholds():
    sql = (ROOT / "streaming" / "ksql" / "streamcart.sql").read_text()
    assert re.findall(r"AMOUNT_USD > (\d+) AND SESSION_AGE_SECONDS < (\d+)", sql) == [
        (str(rules.HIGH_VALUE_USD), str(rules.NEW_SESSION_SECONDS))
    ] * 2
    assert re.findall(r"HAVING COUNT\(\*\) > (\d+)", sql) == [str(rules.VELOCITY_MAX_ORDERS_PER_HOUR)]


def test_regions_seed_covers_every_market_and_currency():
    import csv
    with open(ROOT / "batch" / "dbt" / "seeds" / "regions.csv") as f:
        seed = list(csv.DictReader(f))
    assert sorted(r["local_currency"] for r in seed) == sorted(rules.MARKET_CURRENCIES)
    assert len({r["region_code"] for r in seed}) == len(rules.MARKET_CURRENCIES)
