"""
Bronze layer: Kafka → Iceberg, as a Spark Structured Streaming job.

Records land as produced. Nothing is cleaned or dropped here: the only additions are
ingestion metadata and the partition columns. Tables are partitioned by
region/year/month/day and stored as Snappy Parquet, with Iceberg tracking snapshots
for time travel.
"""

from pathlib import Path

from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import coalesce, col, current_timestamp, dayofmonth, expr, month, to_timestamp, year

from consumer.lakehouse import CATALOG, bucket, create_spark_session
from producer.logging_config import setup_logging

logger = setup_logging("spark_streaming")

SCHEMA_DIR = Path("/opt/airflow/schemas/avro")
TRIGGER = "60 seconds"

# Bronze table → Kafka topic
TABLES = {
    "transactions": "transactions",
    "clickstream": "clickstream",
    "inventory": "inventory_updates",
}


def read_topic(spark, topic: str):
    """Decode one topic against its contract. Confluent frames prefix a 5-byte header."""
    schema = (SCHEMA_DIR / f"{topic}-value.avsc").read_text()
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", topic)
        # On first start, take everything Kafka still holds, including the seed data.
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    decoded = raw.select(
        from_avro(expr("substring(value, 6, length(value) - 5)"), schema).alias("data"),
        col("partition").alias("_kafka_partition"),
        col("offset").alias("_kafka_offset"),
        current_timestamp().alias("_ingested_at"),
    ).select("data.*", "_kafka_partition", "_kafka_offset", "_ingested_at")

    # A malformed or null event time still needs a partition: fall back to arrival time.
    partition_time = coalesce(to_timestamp(col("timestamp")), col("_ingested_at"))
    return (
        decoded.withColumn("year", year(partition_time))
        .withColumn("month", month(partition_time))
        .withColumn("day", dayofmonth(partition_time))
    )


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
