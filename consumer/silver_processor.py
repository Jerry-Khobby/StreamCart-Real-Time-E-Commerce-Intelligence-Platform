"""
Silver layer: incremental Bronze → Silver batch job.

For each table: read new Bronze rows, validate them, clean them, pass the Great
Expectations gate, then MERGE INTO the Silver Iceberg table and publish to Postgres
for dbt. The watermark only moves after a successful write, so a failed run is
simply retried by the next one.

    silver_processor.py                     process rows new since the last run
    silver_processor.py --lookback-days 7   reprocess a window (idempotent, used by the backfill DAG)
"""

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from itertools import chain
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs, broadcast, coalesce, col, concat_ws, count, create_map, first, lag, lit, md5,
    max as spark_max, min as spark_min, row_number, struct, sum as spark_sum, to_json, to_timestamp,
    unix_timestamp, when,
)
from pyspark.sql.window import Window

from consumer.lakehouse import (
    CATALOG, assert_unique, bucket, create_spark_session, jdbc_props, jdbc_url, merge_into,
)
from producer.logging_config import setup_logging
from quality.validation import validate

logger = setup_logging("silver_processing")

VALID_CURRENCIES = ["USD", "EUR", "JPY", "NGN", "BRL"]
VALID_STATUSES = ["completed", "pending", "failed", "refunded"]
VALID_UPDATE_TYPES = ["order_fulfillment", "restock", "damage_writeoff", "audit_adjustment"]
VALID_PAGE_TYPES = ["home", "category", "search", "product", "cart", "checkout",
                    "order_confirmation", "account", "wishlist"]
BOT_UA_PATTERNS = "(?i)bot|python-requests|curl|scrapy|spider|crawler"
# Sessions faster than this are automated. Simulated humans take at least 5s per page.
BOT_MIN_EVENTS = 5
BOT_MAX_AVG_GAP_SECONDS = 3.0
SESSION_GAP_SECONDS = 1800
MAX_EVENT_AGE_SECONDS = 7 * 24 * 3600
FUTURE_TOLERANCE_SECONDS = 300
MAX_AMOUNT_USD = 50_000
# Used when an event predates the first fetched rate, or the rates table is empty.
FALLBACK_FX_RATES = {"USD": 1.00, "EUR": 1.09, "JPY": 0.0067, "NGN": 0.00064, "BRL": 0.20}

SILVER_COLUMNS = {
    "transactions": ["transaction_id", "user_id", "product_id", "category", "region", "ip_region", "currency",
                     "amount_local", "amount_usd", "amount_usd_normalised", "fx_rate_used", "quantity",
                     "payment_method", "status", "session_age_seconds", "event_time", "fx_discrepancy"],
    "clickstream": ["event_id", "session_id", "session_id_silver", "user_id", "region", "page_type", "product_id",
                    "search_query", "referrer", "user_agent", "event_time"],
    "inventory": ["event_id", "product_id", "warehouse_id", "region", "update_type", "quantity_delta",
                  "stock_after", "reorder_point", "event_time"],
}
PRIMARY_KEYS = {"transactions": "transaction_id", "clickstream": "event_id", "inventory": "event_id"}


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------
def read_postgres(spark: SparkSession, query: str) -> DataFrame:
    props = jdbc_props()
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url())
        .option("query", query)
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .load()
    )


def write_silver(df: DataFrame, spark: SparkSession, table: str, primary_key: str):
    """
    Publish rows to silver.<table> in Postgres for dbt.

    Strategy:
      1. Write the batch to a staging table (overwrite)
      2. Upsert from staging into the silver table (INSERT ... ON CONFLICT DO UPDATE)
      3. Drop the staging table, in the same transaction
    """
    url = jdbc_url()
    props = jdbc_props()
    silver_table = f"silver.{table}"
    staging_table = f"silver.{table}_staging"

    (
        df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", staging_table)
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .mode("overwrite")
        .save()
    )

    columns = ", ".join(df.columns)
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in df.columns if c != primary_key)
    with connect_postgres() as conn, conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {silver_table} ({columns})
            SELECT {columns} FROM {staging_table}
            ON CONFLICT ({primary_key}) DO UPDATE SET {update_set}
        """)
        cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
    logger.info(f"[{table}] Published to {silver_table}")


def connect_postgres():
    props = jdbc_props()
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "streamcart_db"),
        user=props["user"],
        password=props["password"],
    )


def record_quality(run_at: datetime, table: str, stage: str, results: list):
    rows = [(run_at, table, stage, r.expectation, r.column, r.success, r.unexpected_count) for r in results]
    with connect_postgres() as conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO observability.quality_results
                (run_at, table_name, stage, expectation, column_name, success, unexpected_count)
            VALUES %s ON CONFLICT DO NOTHING""", rows)


def record_run(run_at: datetime, table: str, rows_in: int, rows_out: int, gate: str):
    with connect_postgres() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO observability.silver_runs (run_at, table_name, rows_in, rows_out, rejected, gate)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",
            (run_at, table, rows_in, rows_out, rows_in - rows_out, gate))


# ---------------------------------------------------------------------------
# Watermarks: tracked on Bronze ingestion time, not event time, so an event that
# arrives late is still picked up.
# ---------------------------------------------------------------------------
def _watermark_path(table: str) -> str:
    return f"s3a://{bucket()}/meta/silver_watermarks/{table}"


def load_watermark(spark: SparkSession, table: str) -> Optional[datetime]:
    try:
        raw = spark.read.text(_watermark_path(table)).head()[0]
        return datetime.fromisoformat(json.loads(raw)["last_ingested_at"])
    except Exception:
        logger.info(f"[{table}] No watermark yet: first run reads all of Bronze")
        return None


def save_watermark(spark: SparkSession, table: str, ts: datetime):
    payload = json.dumps({"last_ingested_at": ts.isoformat(), "table": table})
    spark.createDataFrame([payload], "string").coalesce(1).write.mode("overwrite").text(_watermark_path(table))
    logger.info(f"[{table}] Watermark saved: {ts}")


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------
def read_bronze(spark: SparkSession, table: str) -> DataFrame:
    return spark.table(f"{CATALOG}.bronze.{table}").withColumn("event_time", to_timestamp("timestamp"))


def with_event_age(df: DataFrame) -> DataFrame:
    """
    How old each event was when it reached Bronze, the value the age checks test.

    It uses arrival time, not the current time, so a row gets the same answer however
    long after the fact it's processed. That's what makes a replay reproducible.
    Null for seed data: an explicit backfill is allowed to be older than 7 days.
    """
    age = col("_ingested_at").cast("double") - col("event_time").cast("double")
    return df.withColumn("event_age_seconds", when(~col("is_backfill"), age))


def with_user_known(df: DataFrame, known_users: DataFrame) -> DataFrame:
    """Referential integrity: is user_id a real user in the transactional database?"""
    flagged = known_users.select("user_id").distinct().withColumn("user_known", lit(True))
    return (
        df.join(broadcast(flagged), on="user_id", how="left")
        .withColumn("user_known", coalesce(col("user_known"), lit(False)))
    )


def age_is_acceptable() -> "col":
    return coalesce(
        col("event_age_seconds").between(-FUTURE_TOLERANCE_SECONDS, MAX_EVENT_AGE_SECONDS),
        # A null age is only legitimate on backfill rows that have an event time.
        col("event_time").isNotNull(),
    )


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------
def deduplicate(df: DataFrame, id_col: str) -> DataFrame:
    w = Window.partitionBy(id_col).orderBy(col("_ingested_at").desc(), col("event_time").desc())
    return df.withColumn("_rank", row_number().over(w)).filter(col("_rank") == 1).drop("_rank")


def filter_bots(df: DataFrame) -> DataFrame:
    """Drop sessions with a bot user agent, or that request pages faster than a person could."""
    w = Window.partitionBy("session_id")
    with_rate = (
        df.withColumn("_events", count(lit(1)).over(w))
        .withColumn("_span", col("event_time").cast("double") - spark_min(col("event_time").cast("double")).over(w))
        .withColumn("_max_span", spark_max("_span").over(w))
        .withColumn("_avg_gap", col("_max_span") / (col("_events") - 1))
    )
    too_fast = (col("_events") >= BOT_MIN_EVENTS) & (col("_avg_gap") < BOT_MAX_AVG_GAP_SECONDS)
    return (
        with_rate.filter(~col("user_agent").rlike(BOT_UA_PATTERNS) & ~too_fast)
        .drop("_events", "_span", "_max_span", "_avg_gap")
    )


def stitch_sessions(df: DataFrame) -> DataFrame:
    """
    A user's events belong to one session until a gap of more than 30 minutes.
    The ID hashes the user and the session's first event time, so it stays stable when
    a later run sees the same session again.
    """
    w = Window.partitionBy("user_id").orderBy("event_time")
    stitched = (
        df.withColumn("_prev", lag("event_time").over(w))
        .withColumn("_new", when(
            col("_prev").isNull()
            | (unix_timestamp("event_time") - unix_timestamp("_prev") > SESSION_GAP_SECONDS), 1).otherwise(0))
        .withColumn("_idx", spark_sum("_new").over(w))
    )
    session_window = Window.partitionBy("user_id", "_idx").orderBy("event_time")
    return (
        stitched.withColumn("_start", first("event_time").over(session_window))
        .withColumn("session_id_silver", md5(concat_ws("_", col("user_id"), col("_start").cast("string"))))
        .drop("_prev", "_new", "_idx", "_start")
    )


def normalise_currency(df: DataFrame, fx_rates: DataFrame) -> DataFrame:
    """Convert to USD at the latest hourly rate at or before each transaction."""
    fallback = create_map([lit(x) for x in chain(*FALLBACK_FX_RATES.items())])
    candidates = df.join(
        broadcast(fx_rates.withColumnRenamed("currency", "_fx_currency")),
        (col("currency") == col("_fx_currency")) & (col("fetched_at") <= col("event_time")),
        "left",
    )
    w = Window.partitionBy("transaction_id").orderBy(col("fetched_at").desc_nulls_last())
    latest = candidates.withColumn("_rank", row_number().over(w)).filter(col("_rank") == 1)
    return (
        latest.withColumn("fx_rate_used", coalesce(col("rate_to_usd"), fallback[col("currency")]))
        .withColumn("amount_usd_normalised", col("amount_local") * col("fx_rate_used"))
        .withColumn("fx_discrepancy",
                    spark_abs(col("amount_usd") - col("amount_usd_normalised")) / col("amount_usd_normalised") > 0.01)
        .drop("_rank", "_fx_currency", "rate_to_usd", "fetched_at", "source")
    )


def clean_transactions(df: DataFrame, fx_rates: DataFrame) -> DataFrame:
    cleaned = (
        df.dropna(subset=["transaction_id", "user_id", "product_id", "amount_usd", "amount_local",
                          "event_time", "region", "currency"])
        .filter(col("user_known"))
        .filter(age_is_acceptable())
        .filter(col("currency").isin(VALID_CURRENCIES))
        .filter((col("amount_usd") > 0) & (col("amount_usd") < MAX_AMOUNT_USD) & (col("amount_local") > 0))
        .filter(col("status").isin(VALID_STATUSES))
    )
    return normalise_currency(deduplicate(cleaned, "transaction_id"), fx_rates)


def clean_clickstream(df: DataFrame) -> DataFrame:
    cleaned = (
        df.dropna(subset=["event_id", "session_id", "user_id", "event_time", "region"])
        .filter(col("user_known"))
        .filter(age_is_acceptable())
        .filter(col("page_type").isin(VALID_PAGE_TYPES))
    )
    return stitch_sessions(deduplicate(filter_bots(cleaned), "event_id"))


def clean_inventory(df: DataFrame) -> DataFrame:
    cleaned = (
        df.dropna(subset=["event_id", "product_id", "warehouse_id", "event_time", "region"])
        .filter(age_is_acceptable())
        .filter(col("update_type").isin(VALID_UPDATE_TYPES))
        .filter((col("quantity_delta") != 0) & (col("stock_after") >= 0))
    )
    return deduplicate(cleaned, "event_id")


def rejected_rows(raw: DataFrame, cleaned: DataFrame, key: str, table: str, run_at: datetime) -> DataFrame:
    """
    Rows that didn't reach Silver, kept whole as JSON so they can be inspected or replayed.
    Duplicates of a kept row aren't included: their ID did reach Silver.

    A row's Kafka position identifies it permanently, even when its own key is null or
    corrupted, so reprocessing it always produces the same rejection_id.
    """
    kept = cleaned.select(key).distinct()
    return raw.join(kept, on=key, how="left_anti").select(
        md5(concat_ws("|", lit(table), col("_kafka_partition"), col("_kafka_offset"))).alias("rejection_id"),
        lit(run_at).cast("timestamp").alias("run_at"),
        lit(table).alias("source_table"),
        col(key).alias("record_key"),
        to_json(struct("*")).alias("record"),
    )


def quarantine(spark: SparkSession, rows: DataFrame):
    """
    Add rejected rows to a dead-letter table. They never block the good rows. Rows it
    already holds are left alone, so retries and replays can't create duplicates, and
    run_at stays the run that first rejected the row.
    """
    identifier = f"{CATALOG}.silver.rejected_rows"
    if not spark.catalog.tableExists(identifier):
        (
            rows.limit(0).writeTo(identifier).using("iceberg")
            .partitionedBy(col("source_table"))
            .tableProperty("write.parquet.compression-codec", "snappy")
            .create()
        )
    rows.createOrReplaceTempView("rejected_batch")
    spark.sql(f"""
        MERGE INTO {identifier} AS target
        USING rejected_batch AS source
        ON target.rejection_id = source.rejection_id
        WHEN NOT MATCHED THEN INSERT *
    """)


# ---------------------------------------------------------------------------
# Per-table run
# ---------------------------------------------------------------------------
def ensure_silver_table(spark: SparkSession, table: str, df: DataFrame):
    identifier = f"{CATALOG}.silver.{table}"
    if not spark.catalog.tableExists(identifier):
        (
            df.limit(0).writeTo(identifier).using("iceberg")
            .partitionedBy(col("region"))
            .tableProperty("write.parquet.compression-codec", "snappy")
            .create()
        )


def process_table(spark: SparkSession, table: str, known_users: DataFrame, fx_rates: DataFrame,
                  lookback_days: Optional[int], run_at: datetime) -> str:
    key = PRIMARY_KEYS[table]
    try:
        bronze = read_bronze(spark, table)
    except Exception as exc:
        logger.warning(f"[{table}] Bronze table not ready: {exc}")
        return "skipped"

    watermark = None if lookback_days else load_watermark(spark, table)
    if lookback_days:
        new_rows = bronze.filter(col("_ingested_at") >= lit(run_at - timedelta(days=lookback_days)))
    elif watermark:
        new_rows = bronze.filter(col("_ingested_at") > lit(watermark))
    else:
        new_rows = bronze
    new_rows = new_rows.persist()

    rows_in = new_rows.count()
    if rows_in == 0:
        logger.info(f"[{table}] No new rows")
        new_rows.unpersist()
        return "no_data"

    def prepare(rows: DataFrame) -> DataFrame:
        aged = with_event_age(rows)
        return aged if table == "inventory" else with_user_known(aged, known_users)

    raw = prepare(new_rows).persist()
    record_quality(run_at, table, "raw", validate(raw, table, "raw"))

    if table == "transactions":
        cleaned = clean_transactions(raw, fx_rates)
    elif table == "inventory":
        cleaned = clean_inventory(raw)
    else:
        # Stitching needs each user's recent history, or a session spanning two runs would
        # split. Clean the surrounding two hours too, then keep only this run's new rows.
        oldest = new_rows.agg(spark_min("event_time")).first()[0] or run_at
        context = bronze.filter(col("event_time") >= lit(oldest - timedelta(hours=2)))
        users = new_rows.select("user_id").distinct()
        context = prepare(context.join(users, on="user_id", how="left_semi"))
        cleaned = clean_clickstream(context).join(new_rows.select("event_id"), on="event_id", how="left_semi")

    cleaned = cleaned.persist()
    results = validate(cleaned, table, "clean")
    record_quality(run_at, table, "clean", results)
    rows_out = cleaned.count()

    failed = [f"{r.expectation}({r.column})" for r in results if not r.success]
    if failed:
        logger.error(f"[{table}] Quality gate FAILED, nothing written: {failed}")
        record_run(run_at, table, rows_in, rows_out, "failed")
        return "gate_failed"

    output = cleaned.select(*SILVER_COLUMNS[table])
    if rows_out:
        ensure_silver_table(spark, table, output)
        merge_into(spark, output, f"silver.{table}", key)
        assert_unique(spark, f"silver.{table}", key)
        write_silver(output, spark, table, key)

    quarantine(spark, rejected_rows(raw, cleaned, key, table, run_at))
    if not lookback_days:
        save_watermark(spark, table, new_rows.agg(spark_max("_ingested_at")).first()[0])
    record_run(run_at, table, rows_in, rows_out, "passed")
    logger.info(f"[{table}] in={rows_in:,} out={rows_out:,} rejected={rows_in - rows_out:,}")

    for frame in (new_rows, raw, cleaned):
        frame.unpersist()
    return "passed"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback-days", type=int, default=None, help="Reprocess this window, ignoring the watermark")
    args = parser.parse_args()

    spark = create_spark_session("StreamCart-Silver-Layer")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
    run_at = datetime.now(timezone.utc).replace(microsecond=0)

    known_users = read_postgres(spark, "SELECT user_id FROM oltp.users").cache()
    fx_rates = read_postgres(spark, "SELECT currency, rate_to_usd, fetched_at, source FROM silver.fx_rates").cache()

    outcomes = {
        table: process_table(spark, table, known_users, fx_rates, args.lookback_days, run_at)
        for table in SILVER_COLUMNS
    }
    logger.info(f"Silver run complete: {outcomes}")
    spark.stop()

    # Fail the Airflow task, so a blocked gate is visible rather than a quiet success.
    if "gate_failed" in outcomes.values():
        raise SystemExit(f"Quality gate failed: {outcomes}")


if __name__ == "__main__":
    main()
