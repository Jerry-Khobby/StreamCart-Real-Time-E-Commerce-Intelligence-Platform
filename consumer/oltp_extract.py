"""
Daily batch pull: Postgres (oltp schema) → Bronze Iceberg tables.

Extracts one day of orders and payments, plus a full users snapshot. Writes are MERGE
INTO on the primary key, so rerunning a day is safe.

    oltp_extract.py --date 2026-11-04
"""

import argparse
from datetime import date, timedelta

from pyspark.sql.functions import col, dayofmonth, month, year

from consumer.lakehouse import CATALOG, create_spark_session, jdbc_props, jdbc_url, merge_into
from producer.logging_config import setup_logging

logger = setup_logging("oltp_extract")


def read_query(spark, query: str):
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


def load(spark, df, table: str, key: str):
    """Create the table on first use, partitioned the same way as the rest of Bronze."""
    partitioned = (
        df.withColumn("year", year("created_at"))
        .withColumn("month", month("created_at"))
        .withColumn("day", dayofmonth("created_at"))
    ).persist()
    identifier = f"{CATALOG}.bronze.{table}"
    if not spark.catalog.tableExists(identifier):
        (
            partitioned.limit(0).writeTo(identifier).using("iceberg")
            .partitionedBy(col("region"), col("year"), col("month"), col("day"))
            .tableProperty("write.parquet.compression-codec", "snappy")
            .create()
        )
    merge_into(spark, partitioned, f"bronze.{table}", key)
    logger.info(f"{identifier}: merged {partitioned.count():,} rows")
    partitioned.unpersist()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=date.fromisoformat, help="Day to extract (UTC)")
    args = parser.parse_args()
    start, end = args.date, args.date + timedelta(days=1)

    def window(column: str) -> str:
        return f"{column} >= '{start}' AND {column} < '{end}'"

    spark = create_spark_session("StreamCart-OLTP-Extract")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")

    orders = read_query(spark, f"SELECT * FROM oltp.orders WHERE {window('created_at')}")
    load(spark, orders, "oltp_orders", "order_id")
    # Payments carry their order's region so both tables share one partition layout.
    payments = read_query(spark, f"""
        SELECT p.*, o.region FROM oltp.payments p JOIN oltp.orders o USING (order_id)
        WHERE {window('p.created_at')}
    """)
    load(spark, payments, "oltp_payments", "payment_id")

    users = read_query(spark, "SELECT * FROM oltp.users")
    users.writeTo(f"{CATALOG}.bronze.oltp_users").using("iceberg").createOrReplace()
    logger.info(f"{CATALOG}.bronze.oltp_users: snapshot of {users.count():,} users")
    spark.stop()


if __name__ == "__main__":
    main()
