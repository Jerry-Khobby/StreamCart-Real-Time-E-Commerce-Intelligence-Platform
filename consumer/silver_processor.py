import os
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, lit, row_number, max as spark_max, when,
    year, month, dayofmonth
)
from pyspark.sql.window import Window
from typing import Tuple, Dict
from datetime import datetime, timezone, timedelta
import sys 
sys.path.append("/opt/airflow")
from producer.logging_config import setup_logging

logger = setup_logging("silver_processing")

# Constants
VALID_CURRENCIES   = {"USD", "EUR", "JPY", "NGN", "BRL"}
VALID_STATUSES     = {"completed", "pending", "failed", "refunded"}
VALID_UPDATE_TYPES = {"order_fulfillment", "restock", "damage_writeoff", "audit_adjustment"}
BOT_UA_PATTERNS    = "Googlebot|python-requests|curl|AhrefsBot|scrapy"

WATERMARK_PREFIX   = "s3a://{bucket}/meta/silver_watermarks"

# Spark Session
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StreamCart-Silver-Layer")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint",        "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",      os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key",      os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",            "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

# Watermark helpers
def _watermark_path(bucket: str, table: str) -> str:
    return f"s3a://{bucket}/meta/silver_watermarks/{table}.json"

def load_watermark(spark: SparkSession, bucket: str, table: str) -> datetime:
    default = datetime.now(timezone.utc) - timedelta(days=7)
    path    = _watermark_path(bucket, table)
    try:
        df = spark.read.text(path)
        if df.count() == 0:
            return default
        raw  = df.collect()[0][0]
        data = json.loads(raw)
        ts   = datetime.fromisoformat(data["last_processed"])
        logger.info(f"[{table}] Watermark: {ts}")
        return ts
    except Exception as e:
        logger.info(f"[{table}] First run — defaulting to {default.date()}")
        return default

def save_watermark(spark: SparkSession, bucket: str, table: str, ts: datetime):
    path    = _watermark_path(bucket, table)
    payload = [json.dumps({"last_processed": ts.isoformat(), "table": table})]
    (
        spark.createDataFrame(payload, "string")
             .coalesce(1)
             .write
             .mode("overwrite")
             .text(path)
    )

def add_partition_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("year",  year("event_time"))
          .withColumn("month", month("event_time"))
          .withColumn("day",   dayofmonth("event_time"))
    )

# Incremental Bronze reader
def read_bronze_incremental(
    spark: SparkSession,
    bucket: str,
    table: str,
    watermark: datetime
) -> DataFrame:
    """
    Reads only Bronze partitions newer than the watermark.
    Partition pruning works because Bronze is partitioned by year/month/day.
    """
    path = f"s3a://{bucket}/bronze/{table}"
    logger.info(f"[{table}] Reading bronze from {path} (since {watermark.date()})")

    df = spark.read.parquet(path)

    df = df.withColumn("event_time", to_timestamp("timestamp"))
    df = df.filter(col("event_time") > lit(watermark.isoformat()))

    count = df.count()
    logger.info(f"[{table}] New rows since watermark: {count:,}")
    return df

# Deduplication helper
def deduplicate(df: DataFrame, id_col: str, ts_col: str = "event_time") -> DataFrame:
    """
    Keeps only the latest record per unique ID.
    Uses a window function — idempotent across reruns.
    """
    window = Window.partitionBy(id_col).orderBy(col(ts_col).desc())
    return (
        df.withColumn("_rank", row_number().over(window))
          .filter(col("_rank") == 1)
          .drop("_rank")
    )

# Referential Integrity Checks
def check_referential_integrity(df: DataFrame, spark: SparkSession) -> DataFrame:
    from producer.config import USER_IDS, PRODUCT_IDS

    valid_users    = spark.createDataFrame([(u,) for u in USER_IDS],    ["user_id"])
    valid_products = spark.createDataFrame([(p,) for p in PRODUCT_IDS], ["product_id"])

    df = df.join(valid_users,    on="user_id",    how="inner")
    df = df.join(valid_products, on="product_id", how="inner")
    return df

# TRANSACTIONS
def process_transactions(df: DataFrame, spark: SparkSession) -> Tuple[DataFrame, dict]:
    from pyspark.sql.functions import create_map, lit as spark_lit
    from itertools import chain
    FX_RATES = {"USD": 1.00, "EUR": 1.09, "JPY": 0.0067, "NGN": 0.00064, "BRL": 0.20}
    total = df.count()
    now   = datetime.now(timezone.utc)

    # 0. Referential Checks
    df = check_referential_integrity(df, spark)

    # 1. Required fields
    df = df.dropna(subset=["transaction_id", "user_id", "amount_usd", "timestamp", "region", "currency"])

    # 2. Timestamp already parsed in read_bronze_incremental — just bound-check
    #    No future events (clock skew tolerance: 5 min)
    df = df.filter(col("event_time") <= lit((now + timedelta(minutes=5)).isoformat()))

    # 3. Valid currency (ISO 4217 subset)
    df = df.filter(col("currency").isin(VALID_CURRENCIES))

    # 4. Amount bounds — positive and below fraud ceiling
    df = df.filter((col("amount_usd") > 0) & (col("amount_usd") < 50_000))

    # 5. Valid status
    df = df.filter(col("status").isin(VALID_STATUSES))

    # 6. Deduplicate on transaction_id (idempotent reruns)
    df = deduplicate(df, "transaction_id")

    # 7. Strip internal fraud metadata before Silver
    df = df.drop("_is_fraud", "_fraud_type")

    fx_map = create_map([spark_lit(x) for x in chain(*FX_RATES.items())])

    df = df.withColumn(
        "amount_usd_normalised",
        col("amount_local") * fx_map[col("currency")]
    )
    # Flag rows where pre-computed amount_usd deviates significantly
    df = df.withColumn(
        "fx_discrepancy",
        (abs(col("amount_usd") - col("amount_usd_normalised")) / col("amount_usd_normalised")) > 0.01
    )

    passed = df.count()
    return df, {"total": total, "passed": passed, "rejected": total - passed}

def stitch_sessions(df: DataFrame) -> DataFrame:
    """
    Re-assigns session IDs based on 30-minute inactivity windows per user.
    Overrides the raw session_id from Bronze with a verified one.
    """
    from pyspark.sql.functions import lag, sum as spark_sum, concat_ws, md5
    from pyspark.sql.window import Window

    user_window = Window.partitionBy("user_id").orderBy("event_time")

    # Time gap since previous event for same user
    df = df.withColumn(
        "prev_event_time",
        lag("event_time").over(user_window)
    )
    df = df.withColumn(
        "gap_seconds",
        (col("event_time").cast("long") - col("prev_event_time").cast("long"))
    )

    # New session starts when gap > 30 min or first event
    df = df.withColumn(
        "is_new_session",
        when(col("gap_seconds") > 1800, 1)
        .when(col("prev_event_time").isNull(), 1)
        .otherwise(0)
    )

    # Cumulative session counter per user
    df = df.withColumn(
        "session_index",
        spark_sum("is_new_session").over(user_window)
    )

    # Deterministic session ID from user + session index
    df = df.withColumn(
        "session_id_silver",
        md5(concat_ws("_", col("user_id"), col("session_index").cast("string")))
    )

    return df.drop("prev_event_time", "gap_seconds", "is_new_session", "session_index")

# CLICKSTREAM
def process_clickstream(df: DataFrame, spark: SparkSession) -> Tuple[DataFrame, dict]:
    total = df.count()
    now   = datetime.now(timezone.utc)

    # 1. Required fields
    df = df.dropna(subset=["event_id", "session_id", "user_id", "timestamp", "region"])

    # 2. No future events
    df = df.filter(col("event_time") <= lit((now + timedelta(minutes=5)).isoformat()))

    # 3. Remove bots — tagged flag first (fast), then UA pattern (second pass)
    df = df.filter(col("_is_bot") == False)
    df = df.filter(~col("user_agent").rlike(BOT_UA_PATTERNS))

    # 4. Deduplicate on event_id
    df = deduplicate(df, "event_id")
    df = stitch_sessions(df)

    # 5. Drop internal metadata
    df = df.drop("_is_bot", "_session_age_s")

    passed = df.count()
    return df, {"total": total, "passed": passed, "rejected": total - passed}

# INVENTORY
def process_inventory(df: DataFrame, spark: SparkSession) -> Tuple[DataFrame, dict]:
    total = df.count()
    now   = datetime.now(timezone.utc)

    # 1. Required fields
    df = df.dropna(subset=["event_id", "product_id", "warehouse_id", "timestamp", "region"])

    # 2. No future events
    df = df.filter(col("event_time") <= lit((now + timedelta(minutes=5)).isoformat()))

    # 3. Valid update type
    df = df.filter(col("update_type").isin(VALID_UPDATE_TYPES))

    # 4. Non-zero delta (a zero-delta event is meaningless)
    df = df.filter(col("quantity_delta") != 0)

    # 5. Deduplicate on event_id
    df = deduplicate(df, "event_id")

    passed = df.count()
    return df, {"total": total, "passed": passed, "rejected": total - passed}

# Validation
def run_validation(df: DataFrame, table: str) -> dict:
    results = {}

    if table == "transactions":
        results["no_null_transaction_id"] = df.filter(col("transaction_id").isNull()).count() == 0
        results["no_null_user_id"]        = df.filter(col("user_id").isNull()).count() == 0
        results["amount_usd_positive"]    = df.filter(col("amount_usd") <= 0).count() == 0
        results["amount_usd_bounded"]     = df.filter(col("amount_usd") >= 50_000).count() == 0
        results["currency_valid"]         = df.filter(~col("currency").isin(VALID_CURRENCIES)).count() == 0
        results["status_valid"]           = df.filter(~col("status").isin(VALID_STATUSES)).count() == 0
        results["no_future_events"]       = df.filter(
            col("event_time") > lit((datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat())
        ).count() == 0

    elif table == "clickstream":
        results["no_null_event_id"]   = df.filter(col("event_id").isNull()).count() == 0
        results["no_null_session_id"] = df.filter(col("session_id").isNull()).count() == 0
        results["no_null_user_id"]    = df.filter(col("user_id").isNull()).count() == 0
        results["no_bots"]            = df.filter(col("user_agent").rlike(BOT_UA_PATTERNS)).count() == 0
        results["no_future_events"]   = df.filter(
            col("event_time") > lit((datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat())
        ).count() == 0

    elif table == "inventory":
        results["no_null_event_id"]   = df.filter(col("event_id").isNull()).count() == 0
        results["no_null_product_id"] = df.filter(col("product_id").isNull()).count() == 0
        results["nonzero_delta"]      = df.filter(col("quantity_delta") == 0).count() == 0
        results["valid_update_type"]  = df.filter(~col("update_type").isin(VALID_UPDATE_TYPES)).count() == 0

    passed = all(results.values())

    logger.info(f"\n{'='*55}")
    logger.info(f"Validation Report — {table.upper()}")
    logger.info(f"{'='*55}")
    for check, result in results.items():
        status = "PASS" if result else "FAIL"
        logger.info(f"  [{status}]  {check}")
    logger.info(f"Overall: {'PASSED' if passed else 'FAILED'}")
    logger.info(f"{'='*55}\n")

    return {"table": table, "checks": results, "passed": passed}

# Write Silver — merge mode via partition overwrite (idempotent)
def write_silver(df: DataFrame, bucket: str, table: str):
    """
    Uses partitionOverwriteMode=dynamic so only affected partitions
    are replaced — safe for incremental reruns (idempotent).
    """
    path = f"s3a://{bucket}/silver/{table}"
    logger.info(f"[{table}] Writing silver → {path}")

    (
        df.write
        .option("partitionOverwriteMode", "dynamic")
        .mode("overwrite")
        .partitionBy("region", "year", "month", "day")
        .parquet(path)
    )
    logger.info(f"[{table}] Silver write complete")

# Helper — get max event_time from a processed DataFrame
def get_max_event_time(df: DataFrame) -> datetime:
    row = df.agg(spark_max("event_time").alias("max_ts")).collect()[0]
    if row["max_ts"]:
        return row["max_ts"].replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)

# Main
def main():
    spark  = create_spark_session()
    bucket = os.getenv("MINIO_BUCKET", "streamcart-data")

    tables = [
        ("transactions", process_transactions, "transaction_id"),
        ("clickstream",  process_clickstream,  "event_id"),
        ("inventory",    process_inventory,    "event_id"),
    ]

    all_reports = []

    for table, processor, _ in tables:
        logger.info(f"\n{'#'*60}")
        logger.info(f"Processing: {table.upper()}")
        logger.info(f"{'#'*60}")

        # 1. Load watermark — only process new data
        watermark = load_watermark(spark, bucket, table)

        # 2. Incremental read from Bronze
        bronze_df = read_bronze_incremental(spark, bucket, table, watermark)

        if bronze_df.count() == 0:
            logger.info(f"[{table}] No new data since {watermark} — skipping")
            continue

        # 3. Clean + validate — all processors now accept (df, spark)
        silver_df, stats = processor(bronze_df, spark)
        logger.info(
            f"[{table}] Rows → total: {stats['total']:,} | "
            f"passed: {stats['passed']:,} | "
            f"rejected: {stats['rejected']:,}"
        )

        # 4. Run validation suite
        report = run_validation(silver_df, table)
        all_reports.append(report)

        # 5. Write Silver only if validation passes
        if report["passed"]:
            silver_df = add_partition_columns(silver_df)
            write_silver(silver_df, bucket, table)
            LOCAL_DATA_DIR ="/opt/airflow/data"
            os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
            local_csv_path = os.path.join(LOCAL_DATA_DIR, f"{table}_silver.csv")
            silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(local_csv_path)
            logger.info(f"[{table}] Saved local CSV → {local_csv_path}")

            # 6. Advance watermark to max event_time in this batch
            new_watermark = get_max_event_time(silver_df)
            save_watermark(spark, bucket, table, new_watermark)
        else:
            logger.warning(f"[{table}] Validation FAILED — Silver not written, watermark not advanced")

    logger.info(f"\n{'='*55}")
    logger.info("SILVER PIPELINE COMPLETE")
    logger.info(f"{'='*55}")
    for r in all_reports:
        status = "PASSED" if r["passed"] else "FAILED"
        logger.info(f"  [{status}]  {r['table']}")

    spark.stop()

if __name__ == "__main__":
    main()