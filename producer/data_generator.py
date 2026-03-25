"""
StreamCart Data Generator
=========================
Produces realistic simulated data for:
  - Transactions   → Kafka topic: transactions
  - Clickstream    → Kafka topic: clickstream
  - Inventory      → Kafka topic: inventory_updates

Run standalone (writes JSON to stdout) or with Kafka (set USE_KAFKA=true in .env).

Usage:
    python data_generator.py --mode all       # stream all event types forever
    python data_generator.py --mode tx        # transactions only
    python data_generator.py --mode click     # clickstream only
    python data_generator.py --mode inventory # inventory only
    python data_generator.py --mode backfill --days 30  # historical backfill
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from producer.config import (REGION_WEIGHTS,REGIONS,CURRENCIES,
                             FX_TO_USD,PRODUCT_CATEGORIES,
                             SEASONAL_WEIGHTS,PRICE_RANGES,
                             PAGE_TYPES,PAGE_FUNNEL_WEIGHTS,
                             NUM_PRODUCTS,NUM_USERS,
                             PRODUCT_IDS,USER_IDS,
                             FRAUD_USER_POOL,
                             _SEARCH_TERMS,
                             _REFERRERS,
                             _BOT_UAS,
                             _HUMAN_UAS)
from kafka.errors import KafkaError,NoBrokersAvailable
from producer.logging_config import setup_logging



logger = setup_logging("data_generator") 


# ---------------------------------------------------------------------------
# Optional Kafka import – gracefully degrades to stdout if not available
# ---------------------------------------------------------------------------
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False



# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
USE_KAFKA       = os.getenv("USE_KAFKA", "false").lower() == "true"



import signal

shutdown_flag = False

def handle_shutdown(signum, frame):
    global shutdown_flag
    logger.info(f"Received signal {signum}. Shutting down...")
    shutdown_flag = True

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)



def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def weighted_choice(weights: dict) -> str:
    keys   = list(weights.keys())
    probs  = list(weights.values())
    return random.choices(keys, weights=probs, k=1)[0]


def pick_region() -> str:
    return weighted_choice(REGION_WEIGHTS)


def hourly_tx_weight(hour: int) -> float:
    """
    Transaction volume curve across 24 hours.
    Peak at 10–12 and 19–21 (shopping peaks), trough at 03–05.
    """
    curve = [
        0.2, 0.15, 0.12, 0.10, 0.10, 0.15,  # 00–05
        0.3,  0.5,  0.7,  0.9,  1.0,  1.0,  # 06–11
        0.95, 0.85, 0.8,  0.8,  0.85, 0.9,  # 12–17
        1.0,  1.0,  0.9,  0.75, 0.55, 0.35  # 18–23
    ]
    return curve[hour]


def pick_category(month: int) -> str:
    """Pick a product category weighted by seasonal demand for the given month."""
    weights = {cat: SEASONAL_WEIGHTS[cat][month - 1] for cat in PRODUCT_CATEGORIES}
    return weighted_choice(weights)


def is_fraud_window(user_id: str, ts: datetime) -> tuple[bool, Optional[str]]:
    """
    Returns (is_fraud, fraud_type) for pre-placed fraud patterns.
    Fraud types:
      - card_testing   : many small transactions in quick succession
      - account_takeover: large single transaction from a new session
      - geo_anomaly    : transaction from unexpected region
    """
    if user_id not in FRAUD_USER_POOL:
        return False, None
    # Spread fraud across different hours of the day deterministically
    user_idx  = FRAUD_USER_POOL.index(user_id)
    fraud_hour = (user_idx * 3) % 24
    if ts.hour == fraud_hour:
        fraud_type = ["card_testing", "account_takeover", "geo_anomaly"][user_idx % 3]
        return True, fraud_type
    return False, None


def generate_product_price(category: str, is_fraud: bool, fraud_type: Optional[str]) -> float:
    lo, hi = PRICE_RANGES[category]
    if is_fraud and fraud_type == "card_testing":
        # Small probing amounts
        return round(random.uniform(0.99, 9.99), 2)
    if is_fraud and fraud_type == "account_takeover":
        # Unusually large amount
        return round(random.uniform(hi * 0.8, hi * 1.2), 2)
    return round(random.uniform(lo, hi), 2)


# ---------------------------------------------------------------------------
# Event generators
# ---------------------------------------------------------------------------
def generate_transaction(ts: Optional[datetime] = None) -> dict:
    """Generate a single transaction event."""
    ts        = ts or now_utc()
    region    = pick_region()
    currency  = CURRENCIES[region]
    user_id   = random.choice(USER_IDS)
    product_id = random.choice(PRODUCT_IDS)
    category  = pick_category(ts.month)

    is_fraud, fraud_type = is_fraud_window(user_id, ts)

    amount_local = generate_product_price(category, is_fraud, fraud_type)
    amount_usd   = round(amount_local * FX_TO_USD.get(currency, 1.0), 2)
    quantity     = 1 if is_fraud and fraud_type == "card_testing" else random.randint(1, 5)

    payment_methods = ["credit_card", "debit_card", "paypal", "crypto", "bank_transfer"]
    pm_weights      = [0.45, 0.25, 0.15, 0.08, 0.07]

    status_pool = ["completed", "pending", "failed", "refunded"]
    status_wt   = [0.88, 0.06, 0.04, 0.02]

    event = {
        "event_type":       "transaction",
        "transaction_id":   str(uuid.uuid4()),
        "user_id":          user_id,
        "product_id":       product_id,
        "category":         category,
        "region":           region,
        "currency":         currency,
        "amount_local":     amount_local,
        "amount_usd":       amount_usd,
        "quantity":         quantity,
        "payment_method":   random.choices(payment_methods, weights=pm_weights, k=1)[0],
        "status":           random.choices(status_pool, weights=status_wt, k=1)[0],
        "timestamp":        ts.isoformat(),
        # Fraud metadata – would be stripped in production before landing in Bronze
        "_is_fraud":        is_fraud,
        "_fraud_type":      fraud_type,
    }
    return event


def generate_session(ts: Optional[datetime] = None, max_events: int = 15) -> list[dict]:
    """
    Generate a sequence of clickstream events for one user session.
    Simulates a realistic funnel traversal with occasional bot traffic.
    """
    ts       = ts or now_utc()
    user_id  = random.choice(USER_IDS)
    session_id = str(uuid.uuid4())
    region   = pick_region()
    is_bot   = random.random() < 0.05  # 5% bot traffic

    events   = []
    page     = "home"
    event_ts = ts

    # Bots hammer the same page rapidly; humans browse normally
    num_events = random.randint(2, 4) if is_bot else random.randint(3, max_events)

    for i in range(num_events):
        product_id = random.choice(PRODUCT_IDS) if page == "product" else None
        search_query = _random_search_query() if page == "search" else None

        event = {
            "event_type":    "clickstream",
            "event_id":      str(uuid.uuid4()),
            "session_id":    session_id,
            "user_id":       user_id,
            "region":        region,
            "page_type":     page,
            "product_id":    product_id,
            "search_query":  search_query,
            "referrer":      _random_referrer() if i == 0 else None,
            "user_agent":    _bot_ua() if is_bot else _human_ua(),
            "timestamp":     event_ts.isoformat(),
            "_is_bot":       is_bot,
            "_session_age_s": (event_ts - ts).total_seconds(),
        }
        events.append(event)

        # Advance time within session
        if is_bot:
            event_ts += timedelta(seconds=random.uniform(0.1, 2))
        else:
            event_ts += timedelta(seconds=random.uniform(5, 300))

        # Pick next page according to funnel weights; stop if no onward path
        next_page_weights = PAGE_FUNNEL_WEIGHTS.get(page, {})
        if not next_page_weights:
            break
        page = weighted_choice(next_page_weights)

        # Checkout abandonment: 40% chance of stopping at cart
        if page == "cart" and random.random() < 0.4:
            break

    return events


def generate_inventory_update(ts: Optional[datetime] = None) -> dict:
    """Generate a single inventory update event."""
    ts = ts or now_utc()

    event_types = ["order_fulfillment", "restock", "damage_writeoff", "audit_adjustment"]
    et_weights  = [0.70, 0.20, 0.06, 0.04]
    event_type  = random.choices(event_types, weights=et_weights, k=1)[0]

    product_id  = random.choice(PRODUCT_IDS)
    warehouses  = ["WH-US-EAST", "WH-US-WEST", "WH-EU-CENTRAL", "WH-ASIA-SING", "WH-AF-LAGOS"]

    if event_type == "order_fulfillment":
        delta = -random.randint(1, 10)
    elif event_type == "restock":
        delta = random.randint(50, 500)
    elif event_type == "damage_writeoff":
        delta = -random.randint(1, 30)
    else:
        delta = random.randint(-20, 20)

    return {
        "event_type":    "inventory_update",
        "event_id":      str(uuid.uuid4()),
        "product_id":    product_id,
        "warehouse_id":  random.choice(warehouses),
        "region":        random.choice(REGIONS),
        "update_type":   event_type,
        "quantity_delta": delta,
        "timestamp":     ts.isoformat(),
    }



# Private helpers
def _random_search_query() -> str:
    terms = random.sample(_SEARCH_TERMS, k=random.randint(1, 3))
    return " ".join(terms)


def _random_referrer() -> str:
    return random.choice(_REFERRERS)


def _bot_ua() -> str:
    return random.choice(_BOT_UAS)

def _human_ua() -> str:
    return random.choice(_HUMAN_UAS)


# Kafka producer
def build_producer(retries: int = 5, delay: int = 5):
    if not KAFKA_AVAILABLE:
        logger.warning("kafka-python not installed. Falling back to stdout.")
        return None

    if not USE_KAFKA:
        logger.info("USE_KAFKA is false. Using stdout.")
        return None

    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info("Connected to Kafka")
            return producer

        except NoBrokersAvailable:
            logger.warning(f"Kafka not available (attempt {attempt+1}/{retries})")
            time.sleep(delay)

    logger.error("Failed to connect to Kafka after retries")
    return None


def emit(producer, topic: str, event: dict, retries: int = 3):
    if not producer:
        print(json.dumps(event))
        return

    for attempt in range(retries):
        try:
            future = producer.send(topic, value=event)
            future.get(timeout=10)  # wait for confirmation
            return

        except KafkaError as e:
            logger.warning(f"Send failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)  # exponential backoff

    logger.error("Failed to send message after retries")



# Stream modes
def stream_transactions(producer, tx_per_second: float = 5.0):
    interval = 1.0 / tx_per_second
    print(f"[transactions] Streaming at ~{tx_per_second} tx/s → topic: transactions")
    while not shutdown_flag:
        ts    = now_utc()
        hour  = ts.hour
        weight = hourly_tx_weight(hour)
        # Scale actual sleep to simulate volume variation
        sleep_time = interval / weight
        tx = generate_transaction(ts)
        emit(producer, "transactions", tx)
        time.sleep(sleep_time)


def stream_clickstream(producer, sessions_per_minute: float = 20.0):
    interval = 60.0 / sessions_per_minute
    print(f"[clickstream] Streaming at ~{sessions_per_minute} sessions/min → topic: clickstream")
    while True:
        ts     = now_utc()
        events = generate_session(ts)
        for ev in events:
            emit(producer, "clickstream", ev)
        time.sleep(interval)


def stream_inventory(producer, updates_per_minute: float = 10.0):
    interval = 60.0 / updates_per_minute
    print(f"[inventory] Streaming at ~{updates_per_minute} updates/min → topic: inventory_updates")
    while True:
        ev = generate_inventory_update()
        emit(producer, "inventory_updates", ev)
        time.sleep(interval)



# Backfill mode
def backfill(producer, days: int = 30, tx_per_day: int = 50_000):
    """
    Generate historical data for the past N days.
    Useful for populating Bronze layer from scratch.
    """
    print(f"[backfill] Generating {tx_per_day:,} tx/day × {days} days = {tx_per_day * days:,} total events")
    end   = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    total = 0

    current_day = start
    while current_day < end:
        for _ in range(tx_per_day):
            # Distribute events across the day with hourly weights
            hour   = random.choices(range(24), weights=[hourly_tx_weight(h) for h in range(24)], k=1)[0]
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            ts     = current_day.replace(hour=hour, minute=minute, second=second)
            tx     = generate_transaction(ts)
            emit(producer, "transactions", tx, verbose=False)
            total += 1

        # Also generate a day's worth of inventory snapshots
        for _ in range(int(tx_per_day * 0.02)):
            ts  = current_day + timedelta(hours=random.uniform(0, 24))
            ev  = generate_inventory_update(ts)
            emit(producer, "inventory_updates", ev, verbose=False)
            total += 1

        print(f"  ✓ {current_day.date()} — cumulative events: {total:,}")
        current_day += timedelta(days=1)

    print(f"[backfill] Done. Total events emitted: {total:,}")



# Entry point
def main():
    parser = argparse.ArgumentParser(description="StreamCart data generator")
    parser.add_argument(
        "--mode",
        choices=["all", "tx", "click", "inventory", "backfill", "sample"],
        default="sample",
        help="Generation mode",
    )
    parser.add_argument("--days",    type=int,   default=30,   help="Days for backfill mode")
    parser.add_argument("--tx-rate", type=float, default=5.0,  help="Transactions per second (stream modes)")
    parser.add_argument("--quiet",   action="store_true",       help="Suppress stdout in backfill mode")
    args = parser.parse_args()

    producer = build_producer()

    if args.mode == "sample":
        # Print a few sample events of each type and exit
        print("=== Sample Transaction ===")
        print(json.dumps(generate_transaction(), indent=2))
        print("\n=== Sample Clickstream Session (first event) ===")
        session = generate_session()
        print(json.dumps(session[0], indent=2))
        print(f"  ... ({len(session)} events in session)")
        print("\n=== Sample Inventory Update ===")
        print(json.dumps(generate_inventory_update(), indent=2))
        return

    if args.mode == "backfill":
        backfill(producer, days=args.days)
        return

    # Streaming modes – run concurrently using threads
    import threading

    threads = []

    if args.mode in ("all", "tx"):
        threads.append(threading.Thread(
            target=stream_transactions,
            args=(producer, args.tx_rate),
            daemon=True,
        ))

    if args.mode in ("all", "click"):
        threads.append(threading.Thread(
            target=stream_clickstream,
            args=(producer,),
            daemon=True,
        ))

    if args.mode in ("all", "inventory"):
        threads.append(threading.Thread(
            target=stream_inventory,
            args=(producer,),
            daemon=True,
        ))

    for t in threads:
        t.start()

    print("Generator running. Press Ctrl+C to stop.")
    try:
        while not shutdown_flag:
            time.sleep(1)
    finally:
        logger.info("Flushing producer...")
        if producer:
            producer.flush()
            producer.close()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()