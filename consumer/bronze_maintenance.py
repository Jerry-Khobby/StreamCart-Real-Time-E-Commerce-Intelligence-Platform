"""
Bronze retention and table maintenance, run daily.

  1. Copy rows older than the retention period to the cold-storage bucket, as Parquet
  2. Delete them from the live table, only after the copy has succeeded
  3. Compact the small files that one-minute streaming commits produce
  4. Expire old snapshots (time travel stays available for 7 days)
"""

from datetime import datetime, timedelta, timezone

from pyspark.sql.functions import col, to_timestamp

from consumer.lakehouse import CATALOG, bucket, create_spark_session
from producer.logging_config import setup_logging

logger = setup_logging("bronze_maintenance")

RETENTION_DAYS = 90
SNAPSHOT_RETENTION_DAYS = 7
TABLES = ["transactions", "clickstream", "inventory"]


def archive_expired(spark, table: str, cutoff: datetime):
    identifier = f"{CATALOG}.bronze.{table}"
    expired = spark.table(identifier).filter(to_timestamp(col("timestamp")) < cutoff).persist()
    count = expired.count()
    if count:
        path = f"s3a://{bucket()}-archive/bronze/{table}"
        expired.write.mode("append").option("compression", "snappy").partitionBy("region", "year", "month", "day").parquet(path)
        spark.sql(f"DELETE FROM {identifier} WHERE to_timestamp(timestamp) < timestamp'{cutoff.isoformat()}'")
    logger.info(f"[{table}] Archived and removed {count:,} rows older than {cutoff.date()}")
    expired.unpersist()


def main():
    spark = create_spark_session("StreamCart-Bronze-Maintenance")
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)

    for table in TABLES:
        identifier = f"{CATALOG}.bronze.{table}"
        if not spark.catalog.tableExists(identifier):
            logger.info(f"[{table}] Table not created yet: skipping")
            continue
        archive_expired(spark, table, now - timedelta(days=RETENTION_DAYS))
        spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => 'bronze.{table}')").show(truncate=False)
        expire_before = (now - timedelta(days=SNAPSHOT_RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        spark.sql(f"""
            CALL {CATALOG}.system.expire_snapshots(
                table => 'bronze.{table}', older_than => TIMESTAMP '{expire_before}', retain_last => 10)
        """).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
