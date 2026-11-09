"""
Real-time serving sink: Kafka → Postgres `realtime` schema.

Grafana and the REST API can't query Kafka directly, so this service keeps a small
serving copy of the live topics. Offsets are committed only after the database
commit and every write is an upsert, so a restart can replay without duplicates.
"""

import os
import signal
import time
from datetime import datetime, timedelta, timezone

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from psycopg2.extras import execute_batch

from producer.config import kafka_auth
from producer.logging_config import setup_logging
from quality.rules import FUTURE_TOLERANCE_SECONDS, MAX_AMOUNT_USD, MAX_EVENT_AGE_SECONDS

logger = setup_logging("realtime_sink")

TOPICS = ["transactions", "inventory_updates", "fraud_alerts", "restock_required"]
FLUSH_SECONDS = 2.0
LAG_SAMPLE_SECONDS = 30
PRUNE_SECONDS = 3600
RETENTION = "48 hours"

SQL = {
    "transactions": """
        INSERT INTO realtime.transactions
            (transaction_id, user_id, region, amount_usd, status, event_time, is_fraud, fraud_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING""",
    "fraud_alerts": """
        INSERT INTO realtime.fraud_alerts
            (transaction_id, fraud_signal, user_id, amount_usd, region, event_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id, fraud_signal) DO NOTHING""",
    "inventory_updates": """
        INSERT INTO realtime.stock_levels
            (product_id, warehouse_id, region, stock, reorder_point, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id, warehouse_id) DO UPDATE SET
            stock = EXCLUDED.stock,
            reorder_point = EXCLUDED.reorder_point,
            updated_at = EXCLUDED.updated_at
        WHERE realtime.stock_levels.updated_at <= EXCLUDED.updated_at""",
    "restock_required": """
        INSERT INTO realtime.restock_events
            (event_id, product_id, warehouse_id, region, stock, reorder_point, event_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING""",
}


def parse_time(value):
    """Parse an event timestamp; malformed values return None."""
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def to_row(topic, event):
    """
    Map a Kafka event to (row, event_time), or None if it shouldn't be served.

    Silver is where bad data gets counted and rejected. Here it's just skipped,
    so a bad timestamp or amount can't distort the live dashboards.
    """
    if topic in ("transactions", "inventory_updates") and event.get("is_backfill"):
        return None

    raw_time = event.get("EVENT_TIME") or event.get("timestamp")
    event_time = parse_time(raw_time)
    now = datetime.now(timezone.utc)
    if event_time is None or not now - timedelta(seconds=MAX_EVENT_AGE_SECONDS) <= event_time <= now + timedelta(seconds=FUTURE_TOLERANCE_SECONDS):
        return None

    if topic == "transactions":
        # Same bounds as Silver: one $999,999 fault would swamp the live revenue figure.
        amount = event["amount_usd"]
        if event["user_id"] is None or amount is None or not 0 < amount < MAX_AMOUNT_USD:
            return None
        # The simulator's hidden labels, kept only to score the fraud rules.
        row = (event["transaction_id"], event["user_id"], event["region"],
               event["amount_usd"], event["status"], event_time,
               bool(event.get("_is_fraud")), event.get("_fraud_type"))
    elif topic == "fraud_alerts":
        row = (event["TRANSACTION_ID"], event["FRAUD_SIGNAL"], event["USER_ID"],
               event["AMOUNT_USD"], event["REGION"], event_time)
    elif topic == "inventory_updates":
        row = (event["product_id"], event["warehouse_id"], event["region"],
               event["stock_after"], event["reorder_point"], event_time)
    else:
        row = (event["EVENT_ID"], event["PRODUCT_ID"], event["WAREHOUSE_ID"], event["REGION"],
               event["STOCK_AFTER"], event["REORDER_POINT"], event_time)
    return row, event_time


def flush(conn, consumer, batches):
    if not any(batches.values()):
        return
    with conn.cursor() as cur:
        for topic, rows in batches.items():
            if rows:
                execute_batch(cur, SQL[topic], rows, page_size=200)
    conn.commit()
    consumer.commit(asynchronous=False)
    for rows in batches.values():
        rows.clear()


def sample_lag(conn, consumer, latencies):
    """Record each topic's unconsumed message count and its mean event-to-sink latency."""
    lag_by_topic = dict.fromkeys(TOPICS, 0)
    for tp in consumer.assignment():
        _, high = consumer.get_watermark_offsets(tp, timeout=5, cached=False)
        position = consumer.position([TopicPartition(tp.topic, tp.partition)])[0].offset
        if position >= 0:
            lag_by_topic[tp.topic] += max(high - position, 0)

    sampled_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        for topic, lag in lag_by_topic.items():
            samples = latencies[topic]
            latency = sum(samples) / len(samples) if samples else None
            cur.execute(
                "INSERT INTO realtime.pipeline_lag (sampled_at, topic, consumer_lag, latency_ms) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (sampled_at, topic, lag, latency),
            )
            samples.clear()
    conn.commit()


def prune(conn):
    with conn.cursor() as cur:
        for table, column in [
            ("transactions", "event_time"),
            ("fraud_alerts", "event_time"),
            ("restock_events", "event_time"),
            ("pipeline_lag", "sampled_at"),
        ]:
            cur.execute(f"DELETE FROM realtime.{table} WHERE {column} < now() - interval '{RETENTION}'")
    conn.commit()


def connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def main():
    stop = []
    signal.signal(signal.SIGTERM, lambda *_: stop.append(True))
    signal.signal(signal.SIGINT, lambda *_: stop.append(True))

    registry = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")})
    deserialize = AvroDeserializer(registry)
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": "realtime-sink",
        "enable.auto.commit": False,
        # ksqlDB creates its output topics after this service may have started, so
        # "latest" could miss their first events. Replays are safe: writes are upserts.
        "auto.offset.reset": "earliest",
        "topic.metadata.refresh.interval.ms": 30000,
        "allow.auto.create.topics": False,
        **kafka_auth(),
    })
    consumer.subscribe(TOPICS)
    conn = connect_postgres()
    logger.info(f"Sinking {TOPICS} to Postgres")

    batches = {topic: [] for topic in TOPICS}
    latencies = {topic: [] for topic in TOPICS}
    last_flush = last_lag = last_prune = time.monotonic()

    while not stop:
        msg = consumer.poll(0.5)
        if msg is not None:
            if msg.error():
                # A subscribed topic that doesn't exist yet is expected during startup.
                if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.warning(f"Consumer error: {msg.error()}")
            else:
                topic = msg.topic()
                event = deserialize(msg.value(), SerializationContext(topic, MessageField.VALUE))
                mapped = to_row(topic, event)
                if mapped:
                    row, event_time = mapped
                    batches[topic].append(row)
                    delay = (datetime.now(timezone.utc) - event_time).total_seconds() * 1000
                    latencies[topic].append(max(delay, 0))

        now = time.monotonic()
        if now - last_flush >= FLUSH_SECONDS:
            flush(conn, consumer, batches)
            last_flush = now
        if now - last_lag >= LAG_SAMPLE_SECONDS:
            # Lag is only a metric: a slow broker must not take down the data path.
            try:
                sample_lag(conn, consumer, latencies)
            except KafkaException as exc:
                logger.warning(f"Skipping this lag sample: {exc}")
            last_lag = now
        if now - last_prune >= PRUNE_SECONDS:
            prune(conn)
            last_prune = now

    flush(conn, consumer, batches)
    consumer.close()
    conn.close()
    logger.info("Sink stopped")


if __name__ == "__main__":
    main()
