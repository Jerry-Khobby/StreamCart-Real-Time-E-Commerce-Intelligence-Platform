"""
Bronze retention and table maintenance, run daily.

  1. Copy rows older than the retention period to the cold-storage bucket, as Parquet
  2. Delete them from the live table, only after the copy has succeeded
  3. Compact the small files that one-minute streaming commits produce
  4. Expire old snapshots (time travel stays available for 7 days)

Silver gets steps 3 and 4 as well. Its rows aren't archived, because Bronze can rebuild them.
The dead-letter table keeps rejected rows for the same 90 days as Bronze.
"""

from datetime import datetime, timedelta, timezone

from pyspark.sql.functions import col, to_timestamp

from consumer.lakehouse import CATALOG, bucket, create_spark_session
from producer.logging_config import setup_logging

logger = setup_logging("bronze_maintenance")

RETENTION_DAYS = 90
SNAPSHOT_RETENTION_DAYS = 7
TABLES = ["transactions", "clickstream", "inventory"]
SILVER_TABLES = ["transactions", "clickstream", "inventory", "rejected_rows"]


def archive_expired(spark, table: str, cutoff: datetime):
    identifier = f"{CATALOG}.bronze.{table}"
    expired = spark.table(identifier).filter(to_timestamp(col("timestamp")) < cutoff).persist()
    count = expired.count()
    if count:
        path = f"s3a://{bucket()}-archive/bronze/{table}"
        # At-least-once: if the delete below fails, a retry archives these rows again.
        # Event IDs let a reader of the archive remove any such repeats.
        expired.write.mode("append").option("compression", "snappy").partitionBy("region", "year", "month", "day").parquet(path)
        # The stream appends during this delete. The default serializable isolation
        # treats that as a conflict and aborts, but new rows can't be 90 days old.
        spark.sql(f"ALTER TABLE {identifier} SET TBLPROPERTIES ('write.delete.isolation-level' = 'snapshot')")
        spark.sql(f"DELETE FROM {identifier} WHERE to_timestamp(timestamp) < timestamp'{cutoff.isoformat()}'")
    logger.info(f"[{table}] Archived and removed {count:,} rows older than {cutoff.date()}")
    expired.unpersist()


def compact_and_expire(spark, table: str, now: datetime):
    spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{table}')").show(truncate=False)
    expire_before = (now - timedelta(days=SNAPSHOT_RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""
        CALL {CATALOG}.system.expire_snapshots(
            table => '{table}', older_than => TIMESTAMP '{expire_before}', retain_last => 10)
    """).show(truncate=False)


def main():
    spark = create_spark_session("StreamCart-Bronze-Maintenance")
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)

    for table in TABLES:
        if spark.catalog.tableExists(f"{CATALOG}.bronze.{table}"):
            archive_expired(spark, table, now - timedelta(days=RETENTION_DAYS))
            compact_and_expire(spark, f"bronze.{table}", now)
        else:
            logger.info(f"[{table}] Bronze table not created yet: skipping")

    dead_letter = f"{CATALOG}.silver.rejected_rows"
    if spark.catalog.tableExists(dead_letter):
        cutoff = (now - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        spark.sql(f"ALTER TABLE {dead_letter} SET TBLPROPERTIES ('write.delete.isolation-level' = 'snapshot')")
        spark.sql(f"DELETE FROM {dead_letter} WHERE run_at < TIMESTAMP '{cutoff}'")
        logger.info(f"[rejected_rows] Removed rows older than {RETENTION_DAYS} days")

    for table in SILVER_TABLES:
        if spark.catalog.tableExists(f"{CATALOG}.silver.{table}"):
            compact_and_expire(spark, f"silver.{table}", now)

    spark.stop()


if __name__ == "__main__":
    main()
