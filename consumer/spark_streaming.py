"""
Bronze layer: Kafka → Iceberg, as a Spark Structured Streaming job.

Records land as produced. Nothing is cleaned or dropped here: the only additions are
ingestion metadata and the partition columns. Tables are partitioned by
region/year/month/day and stored as Snappy Parquet, with Iceberg tracking snapshots
for time travel.

Schema evolution: each Kafka message names the schema version it was written with (the
4-byte schema ID after the Confluent magic byte). Every message is decoded with its own
writer schema and then read as the latest registered version, so old and new events land
in the same columns. Fields added by a newer version become new table columns; fields it
dropped stay in the table as nulls. A version registered while the job runs is unknown to
it, so the job stops on the first such message and the restart picks the version up,
replaying that micro-batch from the checkpoint.
"""

import os

from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    coalesce, col, concat, current_timestamp, dayofmonth, expr, lit, month, raise_error, to_timestamp, when, year,
)
from pyspark.sql.types import StructType

from consumer.lakehouse import CATALOG, bucket, create_spark_session
from producer.config import spark_kafka_auth
from producer.logging_config import setup_logging

logger = setup_logging("spark_streaming")

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TRIGGER = "60 seconds"

# Bronze table → Kafka topic
TABLES = {
    "transactions": "transactions",
    "clickstream": "clickstream",
    "inventory": "inventory_updates",
}


def registered_schemas(subject: str) -> tuple[dict[int, str], str]:
    """Every registered version of a subject by schema ID, and the latest one."""
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    versions = [registry.get_version(subject, v) for v in registry.get_versions(subject)]
    writers = {v.schema_id: v.schema.schema_str for v in versions}
    latest = max(versions, key=lambda v: v.version).schema.schema_str
    return writers, latest


def decode(raw: DataFrame, topic: str, writers: dict[int, str], latest: str) -> DataFrame:
    """
    Decode Confluent-framed Avro: a zero magic byte, a 4-byte schema ID, then the payload.
    Each message is read with the schema it was written with and resolved to the latest.
    """
    schema_id = expr("conv(hex(substring(value, 2, 4)), 16, 10)").cast("int")
    payload = expr("substring(value, 6, length(value) - 5)")
    framed = expr("hex(substring(value, 1, 1))") == lit("00")
    data = None
    for writer_id, writer in sorted(writers.items()):
        branch = from_avro(payload, writer, {"avroSchema": latest})
        matches = framed & (schema_id == writer_id)
        data = when(matches, branch) if data is None else data.when(matches, branch)
    data = data.otherwise(raise_error(concat(
        lit(f"{topic}: message written with schema ID "), schema_id.cast("string"),
        lit(", which this job doesn't know yet. Restarting the job loads it."),
    )))
    return raw.select(
        data.alias("data"),
        col("partition").alias("_kafka_partition"),
        col("offset").alias("_kafka_offset"),
        current_timestamp().alias("_ingested_at"),
    ).select("data.*", "_kafka_partition", "_kafka_offset", "_ingested_at")


def read_topic(spark, topic: str):
    writers, latest = registered_schemas(f"{topic}-value")
    logger.info(f"[{topic}] Decoding schema IDs {sorted(writers)}, read as the latest version")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", topic)
        # On first start, take everything Kafka still holds, including the seed data.
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .options(**spark_kafka_auth())
        .load()
    )
    decoded = decode(raw, topic, writers, latest)

    # A malformed or null event time still needs a partition: fall back to arrival time.
    partition_time = coalesce(to_timestamp(col("timestamp")), col("_ingested_at"))
    return (
        decoded.withColumn("year", year(partition_time))
        .withColumn("month", month(partition_time))
        .withColumn("day", dayofmonth(partition_time))
    )


def evolve_table(spark, identifier: str, incoming: StructType):
    """Add the columns a newer schema version brought, and widen promoted types (int to long)."""
    current = {f.name: f.dataType for f in spark.table(identifier).schema.fields}
    for field in incoming.fields:
        ddl_type = field.dataType.simpleString()
        if field.name not in current:
            spark.sql(f"ALTER TABLE {identifier} ADD COLUMN `{field.name}` {ddl_type}")
            logger.info(f"{identifier}: added column {field.name} {ddl_type}")
        elif current[field.name] != field.dataType:
            # Iceberg allows only safe widening and refuses anything else, which stops the job.
            spark.sql(f"ALTER TABLE {identifier} ALTER COLUMN `{field.name}` TYPE {ddl_type}")
            logger.info(f"{identifier}: widened {field.name} to {ddl_type}")


def align_to_table(spark, stream: DataFrame, identifier: str) -> DataFrame:
    """Match the table's columns: ones the latest version dropped are written as nulls."""
    return stream.select([
        col(f.name) if f.name in stream.columns else lit(None).cast(f.dataType).alias(f.name)
        for f in spark.table(identifier).schema.fields
    ])


def ensure_table(spark, table: str, schema):
    """Create the Bronze table from the stream's schema on first run."""
    identifier = f"{CATALOG}.bronze.{table}"
    if not spark.catalog.tableExists(identifier):
        (
            spark.createDataFrame([], schema)
            .writeTo(identifier)
            .using("iceberg")
            .partitionedBy(col("region"), col("year"), col("month"), col("day"))
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "snappy")
            .tableProperty("write.spark.fanout.enabled", "true")
            .create()
        )
        logger.info(f"Created {identifier}")
    return identifier


def main():
    spark = create_spark_session("StreamCart-Bronze-Layer")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")

    for table, topic in TABLES.items():
        stream = read_topic(spark, topic)
        identifier = ensure_table(spark, table, stream.schema)
        evolve_table(spark, identifier, stream.schema)
        stream = align_to_table(spark, stream, identifier)
        (
            stream.writeStream.format("iceberg")
            .outputMode("append")
            .trigger(processingTime=TRIGGER)
            .option("checkpointLocation", f"s3a://{bucket()}/checkpoints/bronze/{table}")
            .toTable(identifier)
        )
        logger.info(f"Streaming {topic} into {identifier}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
