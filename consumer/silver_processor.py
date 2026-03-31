import os
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, lit, row_number, max as spark_max, when,
    year, month, dayofmonth, abs, lag, sum as spark_sum,
    concat_ws, md5, create_map
)
from pyspark.sql.functions import lit as spark_lit
from pyspark.sql.window import Window
from typing import Tuple
from datetime import datetime, timezone, timedelta
from itertools import chain
import sys

sys.path.append("/opt/airflow")
from producer.logging_config import setup_logging

logger = setup_logging("silver_processing")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VALID_CURRENCIES   = {"USD", "EUR", "JPY", "NGN", "BRL"}
VALID_STATUSES     = {"completed", "pending", "failed", "refunded"}
VALID_UPDATE_TYPES = {"order_fulfillment", "restock", "damage_writeoff", "audit_adjustment"}
BOT_UA_PATTERNS    = "Googlebot|python-requests|curl|AhrefsBot|scrapy"
FX_RATES           = {"USD": 1.00, "EUR": 1.09, "JPY": 0.0067, "NGN": 0.00064, "BRL": 0.20}

# ---------------------------------------------------------------------------
# Spark Session
# NOTE: postgres jar is added here so executors can use JDBC write
# ---------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("StreamCart-Silver-Layer")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory",   "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores",  "2")

        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",        os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key",        os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # Packages — hadoop-aws for MinIO, postgresql for JDBC write to Postgres
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.postgresql:postgresql:42.7.3")

        .getOrCreate()
    )


# Postgres helpers
def _jdbc_url() -> str:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db   = os.getenv("POSTGRES_DB",   "streamcart_db")
    return f"jdbc:postgresql://{host}:{port}/{db}"

def _jdbc_props() -> dict:
    return {
        "user":     os.getenv("POSTGRES_USER",     "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "driver":   "org.postgresql.Driver",
    }




# Write Silver — JDBC upsert via INSERT ... ON CONFLICT
def write_silver(df: DataFrame, spark: SparkSession, table: str, primary_key: str):
    """
    Writes a cleaned Spark DataFrame into silver.<table> in Postgres.

    Strategy:
      1. Write full batch to a staging table (overwrite)
      2. Upsert from staging → silver table using JDBC-executed SQL
         (INSERT ... ON CONFLICT DO UPDATE)
      3. Drop staging table
    """
    url         = _jdbc_url()
    props       = _jdbc_props()
    silver_tbl  = f"silver.{table}"
    staging_tbl = f"silver.{table}_staging"

    logger.info(f"[{table}] Writing rows to staging table {staging_tbl}")

    # Step 1 — write to staging (full overwrite, safe to repeat)
    (
        df.write
          .format("jdbc")
          .option("url",      url)
          .option("dbtable",  staging_tbl)
          .option("user",     props["user"])
          .option("password", props["password"])
          .option("driver",   props["driver"])
          # createTableOptions ensures staging uses same PK
          .option("createTableOptions", f"")
          .mode("overwrite")
          .save()
    )
    logger.info(f"[{table}] Staging write complete")

    # Step 2 — upsert from staging into main silver table
    non_pk_cols = [c for c in df.columns if c != primary_key]
    update_set  = ", ".join([f"{c} = EXCLUDED.{c}" for c in non_pk_cols])

    upsert_sql = f"""
        INSERT INTO {silver_tbl}
        SELECT * FROM {staging_tbl}
        ON CONFLICT ({primary_key})
        DO UPDATE SET {update_set}
    """

    # Step 3 — drop staging
    drop_sql = f"DROP TABLE IF EXISTS {staging_tbl}"

    # Execute both via raw JDBC (no psycopg2 needed)
    conn = spark._sc._jvm.java.sql.DriverManager.getConnection(
        url, props["user"], props["password"]
    )
    conn.setAutoCommit(False)
    try:
        stmt = conn.createStatement()
        stmt.execute(upsert_sql)
        stmt.execute(drop_sql)
        conn.commit()
        stmt.close()
        logger.info(f"[{table}] Upsert into {silver_tbl} complete")
    except Exception as e:
        conn.rollback()
        logger.error(f"[{table}] Upsert failed, rolled back: {e}")
        raise
    finally:
        conn.close()



# Watermark helpers
def _watermark_path(bucket: str, table: str) -> str:
    return f"s3a://{bucket}/meta/silver_watermarks/{table}.json"

def load_watermark(spark: SparkSession, bucket: str, table: str) -> datetime:
    default = datetime.now(timezone.utc) - timedelta(hours=1) #reduce the hours to 1 
    try:
        df  = spark.read.text(_watermark_path(bucket, table))
        if len(df.head(1)) == 0:
            return default
        raw  = df.collect()[0][0]
        data = json.loads(raw)
        ts   = datetime.fromisoformat(data["last_processed"])
        logger.info(f"[{table}] Watermark loaded: {ts}")
        return ts
    except Exception:
        logger.info(f"[{table}] First run — defaulting to {default.date()}")
        return default

def save_watermark(spark: SparkSession, bucket: str, table: str, ts: datetime):
    payload = [json.dumps({"last_processed": ts.isoformat(), "table": table})]
    (
        spark.createDataFrame(payload, "string")
             .coalesce(1)
             .write
             .mode("overwrite")
             .text(_watermark_path(bucket, table))
    )
    logger.info(f"[{table}] Watermark saved: {ts}")



# Bronze reader
def read_bronze_incremental(
    spark: SparkSession, bucket: str, table: str, watermark: datetime
) -> DataFrame:
    path = f"s3a://{bucket}/bronze/{table}"
    logger.info(f"[{table}] Reading bronze from {path} (since {watermark.date()})")
    df = (
        spark.read.parquet(path)
             .withColumn("event_time", to_timestamp("timestamp"))
             .filter(col("event_time") > lit(watermark.isoformat()))
    )
    logger.info(f"[{table}] Bronze read complete")
    return df


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------
def deduplicate(df: DataFrame, id_col: str, ts_col: str = "event_time") -> DataFrame:
    w = Window.partitionBy(id_col).orderBy(col(ts_col).desc())
    return (
        df.withColumn("_rank", row_number().over(w))
          .filter(col("_rank") == 1)
          .drop("_rank")
    )



# Referential integrity
def check_referential_integrity(df: DataFrame, spark: SparkSession) -> DataFrame:
    from producer.config import USER_IDS, PRODUCT_IDS
    valid_users    = spark.createDataFrame([(u,) for u in USER_IDS],    ["user_id"])
    valid_products = spark.createDataFrame([(p,) for p in PRODUCT_IDS], ["product_id"])
    return df.join(valid_users, on="user_id", how="inner") \
             .join(valid_products, on="product_id", how="inner")



# Session stitching
def stitch_sessions(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("user_id").orderBy("event_time")
    df = df.withColumn("prev_ts", lag("event_time").over(w))
    df = df.withColumn("gap_s",   (col("event_time").cast("long") - col("prev_ts").cast("long")))
    df = df.withColumn("new_sess",
            when(col("gap_s") > 1800, 1)
            .when(col("prev_ts").isNull(), 1)
            .otherwise(0))
    df = df.withColumn("sess_idx", spark_sum("new_sess").over(w))
    df = df.withColumn("session_id_silver",
            md5(concat_ws("_", col("user_id"), col("sess_idx").cast("string"))))
    return df.drop("prev_ts", "gap_s", "new_sess", "sess_idx")


# ---------------------------------------------------------------------------
# Processors
# ---------------------------------------------------------------------------
def process_transactions(df: DataFrame, spark: SparkSession) -> Tuple[DataFrame, dict]:
    df.cache()
    total = df.count()
    now   = datetime.now(timezone.utc)

    #df = check_referential_integrity(df, spark)
    df = df.dropna(subset=["transaction_id", "user_id", "amount_usd", "timestamp", "region", "currency"])
    df = df.filter(col("event_time") <= lit((now + timedelta(minutes=5)).isoformat()))
    df = df.filter(col("currency").isin(VALID_CURRENCIES))
    df = df.filter((col("amount_usd") > 0) & (col("amount_usd") < 50_000))
    df = df.filter(col("status").isin(VALID_STATUSES))
    df = deduplicate(df, "transaction_id")
    df = df.drop("_is_fraud", "_fraud_type")

    fx_map = create_map([spark_lit(x) for x in chain(*FX_RATES.items())])
    df = df.withColumn("amount_usd_normalised", col("amount_local") * fx_map[col("currency")])
    df = df.withColumn("fx_discrepancy",
            (abs(col("amount_usd") - col("amount_usd_normalised"))
             / col("amount_usd_normalised")) > 0.01)

    # ── Keep only the columns that exist in silver.transactions ──────────
    df = df.select(
        "transaction_id", "user_id", "product_id", "category",
        "region", "currency", "amount_local", "amount_usd",
        "amount_usd_normalised", "quantity", "payment_method",
        "status", "event_time", "fx_discrepancy"
    )
    passed = df.count()

    return df, {"total": total, "passed": passed, "rejected": total - passed}


def process_clickstream(df: DataFrame, spark: SparkSession) -> Tuple[DataFrame, dict]:
    df.cache()
    total = df.count()
    now   = datetime.now(timezone.utc)

    df = df.dropna(subset=["event_id", "session_id", "user_id", "timestamp", "region"])
    df = df.filter(col("event_time") <= lit((now + timedelta(minutes=5)).isoformat()))
    df = df.filter(col("_is_bot") == False)
    df = df.filter(~col("user_agent").rlike(BOT_UA_PATTERNS))
    df = deduplicate(df, "event_id")
    df = stitch_sessions(df)
    df = df.drop("_is_bot", "_session_age_s")

    # ── Keep only the columns that exist in silver.clickstream ───────────
    df = df.select(
        "event_id", "session_id", "session_id_silver", "user_id",
        "region", "page_type", "product_id", "search_query",
        "referrer", "user_agent", "event_time"
    )
    passed = df.count()

    return df, {"total": total, "passed": passed, "rejected": total - passed}


def process_inventory(df: DataFrame, spark: SparkSession) -> Tuple[DataFrame, dict]:
    df.cache()
    total = df.count()
    now   = datetime.now(timezone.utc)

    df = df.dropna(subset=["event_id", "product_id", "warehouse_id", "timestamp", "region"])
    df = df.filter(col("event_time") <= lit((now + timedelta(minutes=5)).isoformat()))
    df = df.filter(col("update_type").isin(VALID_UPDATE_TYPES))
    df = df.filter(col("quantity_delta") != 0)
    df = deduplicate(df, "event_id")

    # ── Keep only the columns that exist in silver.inventory ─────────────
    df = df.select(
        "event_id", "product_id", "warehouse_id",
        "region", "update_type", "quantity_delta", "event_time"
    )

    return df, {"total": total, "passed": df.count(), "rejected": total - df.count()}


# Validation
def run_validation(df: DataFrame, table: str) -> dict:
    results = {}
    now     = datetime.now(timezone.utc)

    if table == "transactions":
        results["no_null_transaction_id"] = df.filter(col("transaction_id").isNull()).count() == 0
        results["no_null_user_id"]        = df.filter(col("user_id").isNull()).count() == 0
        results["amount_usd_positive"]    = df.filter(col("amount_usd") <= 0).count() == 0
        results["amount_usd_bounded"]     = df.filter(col("amount_usd") >= 50_000).count() == 0
        results["currency_valid"]         = df.filter(~col("currency").isin(VALID_CURRENCIES)).count() == 0
        results["status_valid"]           = df.filter(~col("status").isin(VALID_STATUSES)).count() == 0
        results["no_future_events"]       = df.filter(
            col("event_time") > lit((now + timedelta(minutes=5)).isoformat())
        ).count() == 0

    elif table == "clickstream":
        results["no_null_event_id"]   = df.filter(col("event_id").isNull()).count() == 0
        results["no_null_session_id"] = df.filter(col("session_id").isNull()).count() == 0
        results["no_null_user_id"]    = df.filter(col("user_id").isNull()).count() == 0
        results["no_bots"]            = df.filter(col("user_agent").rlike(BOT_UA_PATTERNS)).count() == 0
        results["no_future_events"]   = df.filter(
            col("event_time") > lit((now + timedelta(minutes=5)).isoformat())
        ).count() == 0

    elif table == "inventory":
        results["no_null_event_id"]   = df.filter(col("event_id").isNull()).count() == 0
        results["no_null_product_id"] = df.filter(col("product_id").isNull()).count() == 0
        results["nonzero_delta"]      = df.filter(col("quantity_delta") == 0).count() == 0
        results["valid_update_type"]  = df.filter(~col("update_type").isin(VALID_UPDATE_TYPES)).count() == 0

    passed = all(results.values())
    logger.info(f"\n{'='*55}\nValidation — {table.upper()}\n{'='*55}")
    for check, ok in results.items():
        logger.info(f"  [{'PASS' if ok else 'FAIL'}]  {check}")
    logger.info(f"Overall: {'PASSED' if passed else 'FAILED'}\n{'='*55}")
    return {"table": table, "checks": results, "passed": passed}



# Max event time helper
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

    for table, processor, primary_key in tables:
        logger.info(f"\n{'#'*60}\nProcessing: {table.upper()}\n{'#'*60}")

        # 1. Load watermark
        watermark = load_watermark(spark, bucket, table)

        # 2. Read incremental Bronze
        try:
            bronze_df = read_bronze_incremental(spark, bucket, table, watermark)
            bronze_df.cache()
        except Exception as e:
            logger.warning(f"[{table}] Bronze read failed (no data yet?): {e}")
            continue

        if bronze_df.count() == 0:
            logger.info(f"[{table}] No new data since {watermark} — skipping")
            continue

        # 3. Process
        silver_df, stats = processor(bronze_df, spark)
        logger.info(
            f"[{table}] total={stats['total']:,} "
            f"passed={stats['passed']:,} "
            f"rejected={stats['rejected']:,}"
        )

        if silver_df.count() == 0:
            logger.warning(f"[{table}] All rows rejected — skipping write")
            continue

        # 4. Validate
        report = run_validation(silver_df, table)
        all_reports.append(report)

        # 5. Write to Postgres (silver schema) if validation passes
        if report["passed"]:
            write_silver(silver_df, spark, table, primary_key)
            # 6. Advance watermark only after successful write
            new_watermark = get_max_event_time(silver_df)
            save_watermark(spark, bucket, table, new_watermark)
        else:
            logger.warning(f"[{table}] Validation FAILED — data NOT written to Postgres")

    logger.info(f"\n{'='*55}\nSILVER PIPELINE COMPLETE\n{'='*55}")
    for r in all_reports:
        logger.info(f"  [{'PASSED' if r['passed'] else 'FAILED'}]  {r['table']}")

    spark.stop()


if __name__ == "__main__":
    main()