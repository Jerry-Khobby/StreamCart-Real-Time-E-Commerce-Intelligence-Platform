"""Bronze decoding across schema versions, on real Confluent-framed Avro bytes."""

import io
import json
import struct

import fastavro
import pytest

from consumer.spark_streaming import decode

V1 = {"type": "record", "name": "Order", "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "int"},
    {"name": "channel", "type": ["null", "string"], "default": None},
]}
# v2 adds a field with a default, drops channel and widens amount: all BACKWARD compatible.
V2 = {"type": "record", "name": "Order", "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "long"},
    {"name": "promo_code", "type": ["null", "string"], "default": None},
]}


def frame(schema_id: int, schema: dict, record: dict) -> bytes:
    body = io.BytesIO()
    fastavro.schemaless_writer(body, fastavro.parse_schema(schema), record)
    return b"\x00" + struct.pack(">I", schema_id) + body.getvalue()


def kafka_batch(spark, values):
    return spark.createDataFrame([(v, 0, i) for i, v in enumerate(values)], "value binary, partition int, offset long")


def test_each_message_is_read_with_its_own_version(spark):
    raw = kafka_batch(spark, [
        frame(1, V1, {"order_id": "a", "amount": 5, "channel": "web"}),
        frame(2, V2, {"order_id": "b", "amount": 2**40, "promo_code": "SPRING"}),
    ])
    decoded = decode(raw, "orders", {1: json.dumps(V1), 2: json.dumps(V2)}, json.dumps(V2))
    rows = {r.order_id: r for r in decoded.collect()}
    assert (rows["a"].amount, rows["a"].promo_code) == (5, None)
    assert (rows["b"].amount, rows["b"].promo_code) == (2**40, "SPRING")
    assert "channel" not in decoded.columns


def test_a_version_the_job_does_not_know_stops_it(spark):
    raw = kafka_batch(spark, [frame(3, V2, {"order_id": "c", "amount": 1, "promo_code": None})])
    with pytest.raises(Exception, match="schema ID 3"):
        decode(raw, "orders", {1: json.dumps(V1)}, json.dumps(V1)).collect()
