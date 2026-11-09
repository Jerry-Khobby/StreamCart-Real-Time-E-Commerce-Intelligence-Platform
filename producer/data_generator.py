"""
StreamCart Data Generator
=========================
Simulates the storefront and warehouses:
  - Transactions → Kafka topic transactions, plus orders and payments in Postgres (oltp)
  - Clickstream  → Kafka topic clickstream
  - Inventory    → Kafka topic inventory_updates

Events are Avro-encoded against the Schema Registry contracts and sent to the
partition of their region. With USE_KAFKA=false they print as JSON instead.

Usage:
    python -m producer.data_generator --mode all            # stream everything forever
    python -m producer.data_generator --mode tx             # transactions only
    python -m producer.data_generator --mode click          # clickstream only
    python -m producer.data_generator --mode inventory      # inventory only
    python -m producer.data_generator --mode backfill --days 14 --tx-per-day 2000
    python -m producer.data_generator --mode sample         # print examples and exit
"""

import argparse
import json
import os
import random
import signal
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from producer.config import (
    CURRENCIES, DAY_OF_WEEK_WEIGHTS, FRAUD_USER_POOL, FX_TO_USD, PAGE_FUNNEL_WEIGHTS, PRICE_RANGES,
    PRODUCT_CATEGORIES, PRODUCT_IDS, REGION_PARTITIONS, REGION_WEIGHTS, REGIONS, SEASONAL_WEIGHTS, USER_HOME_REGION,
    USER_IDS, WAREHOUSES, WAREHOUSES_BY_REGION, _BOT_UAS, _HUMAN_UAS, _REFERRERS, _SEARCH_TERMS, kafka_auth,
)
from producer.logging_config import setup_logging
from quality.rules import WET_HOUR_MIN_PRECIPITATION_MM

logger = setup_logging("data_generator")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
SCHEMA_DIR = Path(os.getenv("SCHEMA_DIR", Path(__file__).resolve().parent.parent / "schemas" / "avro"))

# Share of events deliberately corrupted, so the Silver quality checks have real work to do.
DIRTY_RATE = float(os.getenv("DIRTY_RATE", "0.01"))
# Share of clickstream sessions that are bots.
BOT_RATE = 0.05

# Peak orders per second. The demand curves average about 0.4 of the peak on a Sunday,
# so this clears 50,000 orders on every day of the week (about 52,600 on a Sunday).
DEFAULT_TX_RATE = 1.4
# How far behind schedule the order stream may fall before it stops trying to catch up.
MAX_BACKLOG_SECONDS = 30
# Rain keeps people indoors: a wet hour in a market brings it this much more online demand.
RAIN_DEMAND_LIFT = 1.25

# Share of orders placed with a discount code (added in transactions schema version 2).
PROMO_RATE = 0.1
PROMO_CODES = ["WELCOME10", "SPRING15", "FREESHIP", "VIP20"]

# Hard cases, so the fraud rules are tested honestly rather than against fraud built to trip them.
# Share of fraud scenarios crafted to slip under the rules' thresholds.
EVASIVE_FRAUD_RATE = 0.3
# Innocent orders that look suspicious: a customer ordering while abroad, or a returning
# customer making a large purchase straight after logging in.
TRAVELLER_RATE = 0.003
QUICK_BIG_PURCHASE_RATE = 0.001

shutdown_flag = threading.Event()


def handle_shutdown(signum, frame):
    logger.info(f"Received signal {signum}. Shutting down...")
    shutdown_flag.set()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def weighted_choice(weights: dict) -> str:
    keys = list(weights.keys())
    probs = list(weights.values())
    return random.choices(keys, weights=probs, k=1)[0]


def hourly_tx_weight(hour: int) -> float:
    """
    Transaction volume curve across 24 hours.
    Peaks at 10–12 and 18–20 (shopping peaks), trough at 03–05.
    """
    curve = [
        0.2, 0.15, 0.12, 0.10, 0.10, 0.15,  # 00–05
        0.3,  0.5,  0.7,  0.9,  1.0,  1.0,  # 06–11
        0.95, 0.85, 0.8,  0.8,  0.85, 0.9,  # 12–17
        1.0,  1.0,  0.9,  0.75, 0.55, 0.35  # 18–23
    ]
    return curve[hour]


def demand_weight(ts: datetime) -> float:
    """Combined time-of-day and day-of-week demand (weekends dip)."""
    return hourly_tx_weight(ts.hour) * DAY_OF_WEEK_WEIGHTS[ts.weekday()]


def pick_category(month: int) -> str:
    """Pick a product category weighted by seasonal demand for the given month."""
    weights = {cat: SEASONAL_WEIGHTS[cat][month - 1] for cat in PRODUCT_CATEGORIES}
    return weighted_choice(weights)


# ---------------------------------------------------------------------------
# Stock ledger: makes stock_after consistent across orders and restocks
# ---------------------------------------------------------------------------
class StockBook:
    REORDER_POINT = 20

    def __init__(self):
        self._stock = {}
        self._lock = threading.Lock()

    def apply(self, product_id: str, warehouse_id: str, delta: int) -> int:
        with self._lock:
            key = (product_id, warehouse_id)
            # Some SKUs start below the reorder point, so alerts fire from the first minutes.
            current = self._stock.setdefault(key, random.randint(5, 150))
            self._stock[key] = max(current + delta, 0)
            return self._stock[key]

    def lowest(self) -> Optional[tuple[str, str]]:
        """A SKU below its reorder point, if any: what a warehouse would restock first."""
        with self._lock:
            low = [key for key, qty in self._stock.items() if qty < self.REORDER_POINT]
        return random.choice(low) if low else None


# ---------------------------------------------------------------------------
# Event generators
# ---------------------------------------------------------------------------
def generate_transaction(
    ts: Optional[datetime] = None,
    is_backfill: bool = False,
    user_id: Optional[str] = None,
    amount_usd: Optional[float] = None,
    session_age_seconds: Optional[float] = None,
    ip_region: Optional[str] = None,
    quantity: Optional[int] = None,
    fraud_type: Optional[str] = None,
) -> dict:
    """Generate one order. Prices are set in USD, then expressed in the market's currency."""
    ts = ts or now_utc()
    user_id = user_id or random.choice(USER_IDS)
    region = USER_HOME_REGION[user_id]
    currency = CURRENCIES[region]
    category = pick_category(ts.month)

    if amount_usd is None:
        low, high = PRICE_RANGES[category]
        amount_usd = random.uniform(low, high)
    amount_usd = round(amount_usd, 2)

    payment_methods = ["credit_card", "debit_card", "paypal", "crypto", "bank_transfer"]
    pm_weights = [0.45, 0.25, 0.15, 0.08, 0.07]
    status_pool = ["completed", "pending", "failed", "refunded"]
    status_wt = [0.88, 0.06, 0.04, 0.02]

    return {
        "event_type": "transaction",
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "product_id": random.choice(PRODUCT_IDS),
        "category": category,
        "region": region,
        "ip_region": ip_region or region,
        "currency": currency,
        "amount_local": round(amount_usd / FX_TO_USD[currency], 2),
        "amount_usd": amount_usd,
        "quantity": quantity or random.randint(1, 5),
        "payment_method": random.choices(payment_methods, weights=pm_weights, k=1)[0],
        "status": random.choices(status_pool, weights=status_wt, k=1)[0],
        "session_age_seconds": round(session_age_seconds if session_age_seconds is not None
                                     else random.uniform(60, 1800), 1),
        "timestamp": ts.isoformat(),
        "is_backfill": is_backfill,
        # Ground truth for scoring detection; dropped in Silver.
        "_is_fraud": fraud_type is not None,
        "_fraud_type": fraud_type,
        # Schema version 2: a discount code on some orders
        "promo_code": random.choice(PROMO_CODES) if random.random() < PROMO_RATE else None,
    }


def generate_fraud_scenario(
    ts: Optional[datetime] = None,
    is_backfill: bool = False,
    scenario: Optional[str] = None,
    evasive: Optional[bool] = None,
) -> list[dict]:
    """
    A pre-placed fraud window. Each scenario normally trips one real-time rule:
      card_testing      a burst of small charges from one user → VELOCITY_BREACH
      account_takeover  a very large order moments after login → HIGH_VALUE_NEW_SESSION
      geo_anomaly       an order from outside the user's market → GEO_ANOMALY

    About 30% are evasive and slip under the rules, as real fraudsters adapt:
      card_testing      no more than 10 charges spread over most of an hour
      account_takeover  an amount just under the $5,000 threshold
      geo_anomaly       routed through the customer's own market, as with a VPN
    """
    ts = ts or now_utc()
    user_id = random.choice(FRAUD_USER_POOL)
    scenario = scenario or random.choice(["card_testing", "account_takeover", "geo_anomaly"])
    evasive = random.random() < EVASIVE_FRAUD_RATE if evasive is None else evasive

    if scenario == "card_testing":
        count, spacing = (random.randint(6, 10), (240, 330)) if evasive else (random.randint(12, 18), (2, 10))
        return [
            generate_transaction(
                ts - timedelta(seconds=i * random.uniform(*spacing)), is_backfill, user_id,
                amount_usd=random.uniform(0.99, 9.99), quantity=1, fraud_type=scenario,
            )
            for i in range(count)
        ]
    if scenario == "account_takeover":
        amount = random.uniform(3000, 4999) if evasive else random.uniform(5001, 15000)
        return [generate_transaction(
            ts, is_backfill, user_id, amount_usd=amount,
            session_age_seconds=random.uniform(3, 55), fraud_type=scenario,
        )]
    home = USER_HOME_REGION[user_id]
    origin = home if evasive else random.choice([r for r in REGIONS if r != home])
    return [generate_transaction(ts, is_backfill, user_id, ip_region=origin, fraud_type=scenario)]


USERS_BY_REGION = {region: [u for u in USER_IDS if USER_HOME_REGION[u] == region] for region in REGIONS}


def pick_customer(region_lift: Optional[dict] = None) -> str:
    """A random customer. A market with a demand lift (rain) is proportionally more likely."""
    if not region_lift:
        return random.choice(USER_IDS)
    weights = [REGION_WEIGHTS[r] * region_lift.get(r, 1.0) for r in REGIONS]
    return random.choice(USERS_BY_REGION[random.choices(REGIONS, weights=weights, k=1)[0]])


def overall_lift(region_lift: Optional[dict]) -> float:
    """How much the lifts raise total demand, so rain adds orders rather than moving them."""
    if not region_lift:
        return 1.0
    return sum(REGION_WEIGHTS[r] * region_lift.get(r, 1.0) for r in REGIONS) / sum(REGION_WEIGHTS.values())


def generate_customer_order(
    ts: Optional[datetime] = None, is_backfill: bool = False, region_lift: Optional[dict] = None,
) -> dict:
    """An ordinary order. A small share look suspicious but are innocent: false alarms in waiting."""
    roll = random.random()
    if roll < TRAVELLER_RATE:
        user_id = random.choice(USER_IDS)
        abroad = random.choice([r for r in REGIONS if r != USER_HOME_REGION[user_id]])
        return generate_transaction(ts, is_backfill, user_id, ip_region=abroad)
    if roll < TRAVELLER_RATE + QUICK_BIG_PURCHASE_RATE:
        return generate_transaction(
            ts, is_backfill, amount_usd=random.uniform(5001, 9000), session_age_seconds=random.uniform(10, 55)
        )
    return generate_transaction(ts, is_backfill, pick_customer(region_lift))


def corrupt_transaction(event: dict) -> dict:
    """Inject one realistic data fault. Silver has to catch every one of these."""
    fault = random.choice([
        "null_user", "unknown_user", "negative_amount", "huge_amount",
        "bad_currency", "future_event", "stale_event", "null_timestamp",
    ])
    event = dict(event)
    if fault == "null_user":
        event["user_id"] = None
    elif fault == "unknown_user":
        event["user_id"] = "USER-9999999"
    elif fault == "negative_amount":
        event["amount_usd"] = -abs(event["amount_usd"])
    elif fault == "huge_amount":
        event["amount_usd"] = 999_999.0
    elif fault == "bad_currency":
        event["currency"] = "XXX"
    elif fault == "future_event":
        event["timestamp"] = (now_utc() + timedelta(days=2)).isoformat()
    elif fault == "stale_event":
        event["timestamp"] = (now_utc() - timedelta(days=10)).isoformat()
    else:
        event["timestamp"] = None
    return event


def generate_session(ts: Optional[datetime] = None, is_backfill: bool = False, max_events: int = 15) -> list[dict]:
    """
    Generate the clickstream events of one session, ending at `ts`.

    Bots carry no label. Half announce themselves in their user agent; the other half
    spoof a browser, and can only be caught by their request rate.
    """
    ts = ts or now_utc()
    user_id = random.choice(USER_IDS)
    session_id = str(uuid.uuid4())
    region = USER_HOME_REGION[user_id]
    is_bot = random.random() < BOT_RATE
    spoofed = is_bot and random.random() < 0.5
    user_agent = random.choice(_HUMAN_UAS) if (not is_bot or spoofed) else random.choice(_BOT_UAS)
    num_events = random.randint(6, 20) if is_bot else random.randint(3, max_events)

    pages, offsets = [], []
    page, offset = "home", 0.0
    for _ in range(num_events):
        pages.append(page)
        offsets.append(offset)
        offset += random.uniform(0.1, 2) if is_bot else random.uniform(5, 300)

        next_page_weights = PAGE_FUNNEL_WEIGHTS.get(page, {})
        if not next_page_weights:
            break
        page = weighted_choice(next_page_weights)
        # Checkout abandonment: 40% of sessions stop at the cart.
        if page == "cart" and not is_bot and random.random() < 0.4:
            pages.append(page)
            offsets.append(offset)
            break

    # A session that finishes "now" started earlier, so no event is in the future.
    start = ts - timedelta(seconds=offsets[-1])
    return [
        {
            "event_type": "clickstream",
            "event_id": str(uuid.uuid4()),
            "session_id": session_id,
            "user_id": user_id,
            "region": region,
            "page_type": page_type,
            "product_id": random.choice(PRODUCT_IDS) if page_type == "product" else None,
            "search_query": _random_search_query() if page_type == "search" else None,
            "referrer": random.choice(_REFERRERS) if i == 0 else None,
            "user_agent": user_agent,
            "timestamp": (start + timedelta(seconds=offsets[i])).isoformat(),
            "is_backfill": is_backfill,
        }
        for i, page_type in enumerate(pages)
    ]


def generate_inventory_update(
    stock: StockBook,
    ts: Optional[datetime] = None,
    is_backfill: bool = False,
    update_type: Optional[str] = None,
    product_id: Optional[str] = None,
    warehouse_id: Optional[str] = None,
    delta: Optional[int] = None,
) -> dict:
    """Generate one stock movement. Called with no overrides, it picks a non-order movement."""
    ts = ts or now_utc()
    if update_type is None:
        update_type = random.choices(["restock", "damage_writeoff", "audit_adjustment"], weights=[0.7, 0.2, 0.1], k=1)[0]
    if update_type == "restock" and product_id is None:
        low = stock.lowest()
        if low:
            product_id, warehouse_id = low
    product_id = product_id or random.choice(PRODUCT_IDS)
    warehouse_id = warehouse_id or random.choice(list(WAREHOUSES))

    if delta is None:
        if update_type == "restock":
            delta = random.randint(50, 500)
        elif update_type == "damage_writeoff":
            delta = -random.randint(1, 30)
        else:
            delta = random.choice([d for d in range(-20, 21) if d != 0])

    return {
        "event_type": "inventory_update",
        "event_id": str(uuid.uuid4()),
        "product_id": product_id,
        "warehouse_id": warehouse_id,
        "region": WAREHOUSES[warehouse_id],
        "update_type": update_type,
        "quantity_delta": delta,
        "stock_after": stock.apply(product_id, warehouse_id, delta),
        "reorder_point": StockBook.REORDER_POINT,
        "timestamp": ts.isoformat(),
        "is_backfill": is_backfill,
    }


def fulfil_order(stock: StockBook, tx: dict, ts: datetime, is_backfill: bool = False) -> Optional[dict]:
    """Orders reduce stock at a warehouse in the customer's market. Failed orders ship nothing."""
    if tx["status"] == "failed":
        return None
    return generate_inventory_update(
        stock, ts, is_backfill, update_type="order_fulfillment", product_id=tx["product_id"],
        warehouse_id=random.choice(WAREHOUSES_BY_REGION[tx["region"]]), delta=-tx["quantity"],
    )


def _random_search_query() -> str:
    return " ".join(random.sample(_SEARCH_TERMS, k=random.randint(1, 3)))


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------
class EventPublisher:
    """Avro producer that enforces the registered contracts and routes by region."""

    TOPICS = {"transactions", "clickstream", "inventory_updates"}

    def __init__(self):
        # Imported here so --mode sample and the unit tests need no Kafka client.
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        self._context = lambda topic: SerializationContext(topic, MessageField.VALUE)
        registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        # auto.register=False is what makes the registry a contract: a producer whose
        # event doesn't fit the registered schema fails instead of changing the schema.
        conf = {"auto.register.schemas": False, "use.latest.version": True}
        self._serializers = {
            topic: AvroSerializer(registry, (SCHEMA_DIR / f"{topic}-value.avsc").read_text(), conf=conf)
            for topic in self.TOPICS
        }
        self._producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            **kafka_auth(),
        })
        logger.info(f"Publishing Avro events to {KAFKA_BOOTSTRAP}")

    def send(self, topic: str, event: dict):
        value = self._serializers[topic](event, self._context(topic))
        while True:
            try:
                self._producer.produce(
                    topic, key=event["region"], value=value,
                    partition=REGION_PARTITIONS[event["region"]],
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                self._producer.poll(1)
        self._producer.poll(0)

    @staticmethod
    def _on_delivery(err, msg):
        if err:
            logger.error(f"Delivery to {msg.topic()} failed: {err}")

    def flush(self):
        self._producer.flush(30)


class StdoutPublisher:
    def send(self, topic: str, event: dict):
        print(json.dumps({"topic": topic, **event}))

    def flush(self):
        pass


def connect_postgres():
    import psycopg2
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def rain_lift(precipitation_by_region: dict) -> dict:
    """Demand multiplier per market from its latest rainfall."""
    return {
        region: RAIN_DEMAND_LIFT if (mm or 0) >= WET_HOUR_MIN_PRECIPITATION_MM else 1.0
        for region, mm in precipitation_by_region.items()
    }


class WeatherBook:
    """
    The latest real weather per market, from the hourly Open-Meteo pull in silver.weather_hourly.
    With no recent reading (the weather DAG hasn't run, or Postgres is unreachable) demand is unchanged.
    """

    REFRESH_SECONDS = 600

    def __init__(self):
        self._lift: dict = {}
        self._loaded_at = float("-inf")

    def lift(self) -> dict:
        if time.monotonic() - self._loaded_at >= self.REFRESH_SECONDS:
            self._loaded_at = time.monotonic()
            self._lift = rain_lift(self._latest_precipitation())
            wet = sorted(r for r, v in self._lift.items() if v > 1)
            logger.info(f"[weather] Wet markets: {', '.join(wet) or 'none'}")
        return self._lift

    @staticmethod
    def _latest_precipitation() -> dict:
        try:
            conn = connect_postgres()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT ON (region) region, precipitation_mm
                        FROM silver.weather_hourly
                        WHERE observed_at > (now() AT TIME ZONE 'UTC') - interval '3 hours'
                        ORDER BY region, observed_at DESC""")
                    return dict(cur.fetchall())
            finally:
                conn.close()
        except Exception as exc:
            logger.warning(f"[weather] No weather available, demand unchanged: {exc}")
            return {}


class OltpWriter:
    """
    Writes orders and payments to the transactional database, the way a storefront would.
    Only well-formed orders land here: the system of record doesn't contain the
    corrupted events injected into the event stream.
    """

    def __init__(self, batch_size: int = 50):
        from psycopg2.extras import execute_values

        self._execute_values = execute_values
        self._conn = connect_postgres()
        self._batch_size = batch_size
        self._orders, self._payments = [], []
        self._lock = threading.Lock()

    def record(self, tx: dict):
        with self._lock:
            self._orders.append((tx["transaction_id"], tx["user_id"], tx["product_id"], tx["region"],
                                 tx["quantity"], tx["status"], tx["timestamp"]))
            self._payments.append((str(uuid.uuid4()), tx["transaction_id"], tx["payment_method"],
                                   tx["currency"], tx["amount_local"], tx["timestamp"]))
            if len(self._orders) >= self._batch_size:
                self._flush_locked()

    def flush(self):
        with self._lock:
            self._flush_locked()

    def _flush_locked(self):
        if not self._orders:
            return
        with self._conn.cursor() as cur:
            self._execute_values(cur, """
                INSERT INTO oltp.orders (order_id, user_id, product_id, region, quantity, status, created_at)
                VALUES %s ON CONFLICT DO NOTHING""", self._orders)
            self._execute_values(cur, """
                INSERT INTO oltp.payments (payment_id, order_id, method, currency, amount_local, created_at)
                VALUES %s ON CONFLICT DO NOTHING""", self._payments)
        self._conn.commit()
        self._orders, self._payments = [], []

    def has_history(self) -> bool:
        """True if orders older than a day exist, meaning the seed has already run."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT 1 FROM oltp.orders WHERE created_at < now() - interval '1 day')")
            return cur.fetchone()[0]


def build_publisher(retries: int = 12, delay: int = 5):
    if not USE_KAFKA:
        logger.info("USE_KAFKA is false. Printing events to stdout.")
        return StdoutPublisher()
    for attempt in range(retries):
        try:
            return EventPublisher()
        except Exception as exc:
            logger.warning(f"Kafka/registry not ready (attempt {attempt + 1}/{retries}): {exc}")
            time.sleep(delay)
    raise SystemExit("Could not connect to Kafka and the Schema Registry")


def emit_transaction(publisher, oltp, stock, tx: dict, ts: datetime, is_backfill: bool = False):
    """Publish an order, record it in the OLTP database and ship it from a warehouse."""
    dirty = not is_backfill and random.random() < DIRTY_RATE
    publisher.send("transactions", corrupt_transaction(tx) if dirty else tx)
    # A retried message: Silver's deduplication has to collapse it.
    if not is_backfill and random.random() < DIRTY_RATE:
        publisher.send("transactions", tx)
    if oltp:
        oltp.record(tx)
    update = fulfil_order(stock, tx, ts, is_backfill)
    if update:
        publisher.send("inventory_updates", update)


# ---------------------------------------------------------------------------
# Stream modes
# ---------------------------------------------------------------------------
def next_deadline(deadline: float, interval: float, now: float) -> float:
    """
    When the next order is due. Pacing against a schedule, not sleeping a full interval
    after each send, stops slow sends from lowering the rate. A stream that falls far
    behind (the machine was busy) catches up at most MAX_BACKLOG_SECONDS of orders.
    """
    return max(deadline + interval, now - MAX_BACKLOG_SECONDS)


def stream_transactions(publisher, oltp, stock, tx_per_second: float, weather: Optional[WeatherBook] = None):
    logger.info(f"[transactions] Streaming at up to {tx_per_second} tx/s")
    deadline = time.monotonic()
    while not shutdown_flag.is_set():
        ts = now_utc()
        lift = weather.lift() if weather else {}
        emit_transaction(publisher, oltp, stock, generate_customer_order(ts, region_lift=lift), ts)
        interval = 1.0 / (tx_per_second * demand_weight(ts) * overall_lift(lift))
        deadline = next_deadline(deadline, interval, time.monotonic())
        shutdown_flag.wait(max(0.0, deadline - time.monotonic()))


def stream_fraud(publisher, oltp, stock, per_hour: float = 20.0):
    logger.info(f"[fraud] Injecting about {per_hour} fraud scenarios/hour")
    while not shutdown_flag.wait(random.expovariate(per_hour / 3600)):
        ts = now_utc()
        for tx in generate_fraud_scenario(ts):
            emit_transaction(publisher, oltp, stock, tx, ts)


def stream_clickstream(publisher, sessions_per_minute: float = 20.0):
    logger.info(f"[clickstream] Streaming about {sessions_per_minute} sessions/min")
    while not shutdown_flag.is_set():
        for event in generate_session():
            publisher.send("clickstream", event)
        shutdown_flag.wait(60.0 / sessions_per_minute)


def stream_inventory(publisher, stock, updates_per_minute: float = 10.0):
    logger.info(f"[inventory] Streaming about {updates_per_minute} non-order updates/min")
    while not shutdown_flag.is_set():
        publisher.send("inventory_updates", generate_inventory_update(stock))
        shutdown_flag.wait(60.0 / updates_per_minute)


def backfill(publisher, oltp, stock, days: int = 14, tx_per_day: int = 2000):
    """Seed history, so batch dashboards have trends on first start. Runs once."""
    if oltp and oltp.has_history():
        logger.info("[backfill] History already exists. Nothing to do.")
        return

    logger.info(f"[backfill] Seeding {days} days at about {tx_per_day:,} orders/day")
    end = now_utc().replace(hour=0, minute=0, second=0, microsecond=0)
    hours = range(24)
    hour_weights = [hourly_tx_weight(h) for h in hours]

    def random_moment(day: datetime) -> datetime:
        hour = random.choices(hours, weights=hour_weights, k=1)[0]
        return day + timedelta(hours=hour, seconds=random.randint(0, 3599))

    total = 0
    for offset in range(days, 0, -1):
        day = end - timedelta(days=offset)
        orders = int(tx_per_day * DAY_OF_WEEK_WEIGHTS[day.weekday()])

        for _ in range(orders):
            ts = random_moment(day)
            emit_transaction(publisher, oltp, stock, generate_customer_order(ts, True), ts, True)
        for _ in range(5):
            ts = random_moment(day)
            for tx in generate_fraud_scenario(ts, True):
                emit_transaction(publisher, oltp, stock, tx, ts, True)
        for _ in range(orders // 2):
            for event in generate_session(random_moment(day), True):
                publisher.send("clickstream", event)
        for _ in range(orders // 10):
            publisher.send("inventory_updates", generate_inventory_update(stock, random_moment(day), True))

        total += orders
        publisher.flush()
        if oltp:
            oltp.flush()
        logger.info(f"[backfill] {day.date()} done, {total:,} orders so far")
    logger.info("[backfill] Complete")


def main():
    parser = argparse.ArgumentParser(description="StreamCart data generator")
    parser.add_argument("--mode", choices=["all", "tx", "click", "inventory", "backfill", "sample"], default="sample")
    parser.add_argument("--days", type=int, default=14, help="Days of history for backfill mode")
    parser.add_argument("--tx-per-day", type=int, default=2000, help="Orders per day for backfill mode")
    parser.add_argument("--tx-rate", type=float, default=DEFAULT_TX_RATE, help="Peak transactions per second (stream modes)")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    stock = StockBook()

    if args.mode == "sample":
        print(json.dumps(generate_transaction(), indent=2))
        print(json.dumps(generate_session()[0], indent=2))
        print(json.dumps(generate_inventory_update(stock), indent=2))
        return

    publisher = build_publisher()
    oltp = OltpWriter() if USE_KAFKA else None

    if args.mode == "backfill":
        backfill(publisher, oltp, stock, days=args.days, tx_per_day=args.tx_per_day)
        publisher.flush()
        return

    jobs = []
    if args.mode in ("all", "tx"):
        jobs.append((stream_transactions, (publisher, oltp, stock, args.tx_rate, WeatherBook() if oltp else None)))
        jobs.append((stream_fraud, (publisher, oltp, stock)))
    if args.mode in ("all", "click"):
        jobs.append((stream_clickstream, (publisher,)))
    if args.mode in ("all", "inventory"):
        jobs.append((stream_inventory, (publisher, stock)))

    threads = [threading.Thread(target=target, args=job_args, daemon=True) for target, job_args in jobs]
    for thread in threads:
        thread.start()

    logger.info("Generator running. Press Ctrl+C to stop.")
    # The OLTP buffer flushes on size or here, whichever comes first.
    while not shutdown_flag.wait(5):
        if not all(thread.is_alive() for thread in threads):
            raise SystemExit("A generator thread died; exiting so the container restarts")
        if oltp:
            oltp.flush()

    for thread in threads:
        thread.join(timeout=5)
    publisher.flush()
    if oltp:
        oltp.flush()
    logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
