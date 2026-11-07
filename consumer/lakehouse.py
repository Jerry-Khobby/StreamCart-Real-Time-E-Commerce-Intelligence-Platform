"""Spark session shared by every lakehouse job: Iceberg on MinIO, cataloged in Postgres."""

import os

from pyspark.sql import DataFrame, SparkSession

CATALOG = "lake"

# Spark 3.5.0 can lose join matches on cached data when adaptive execution changes its
# partitioning (SPARK-45282). A MERGE from a cached DataFrame then kept and re-inserted
# about half the rows it should have updated, with no error.
SPARK_SAFETY_CONF = {"spark.sql.optimizer.canChangeCachedPlanOutputPartitioning": "false"}


def bucket() -> str:
    return os.getenv("MINIO_BUCKET", "streamcart-data")


def jdbc_url() -> str:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "streamcart_db")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def jdbc_props() -> dict:
    return {
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver",
    }


def create_spark_session(app_name: str) -> SparkSession:
    props = jdbc_props()
    builder = SparkSession.builder.appName(app_name)
    for key, value in SPARK_SAFETY_CONF.items():
        builder = builder.config(key, value)
    session = (
        builder
        .config("spark.sql.session.timeZone", "UTC")
        # MinIO through S3A
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Iceberg. The JDBC catalog gives atomic table commits, which S3-style storage
        # can't provide by itself (it has no atomic rename).
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "jdbc")
        .config(f"spark.sql.catalog.{CATALOG}.uri", jdbc_url())
        .config(f"spark.sql.catalog.{CATALOG}.jdbc.user", props["user"])
        .config(f"spark.sql.catalog.{CATALOG}.jdbc.password", props["password"])
        .config(f"spark.sql.catalog.{CATALOG}.jdbc.schema-version", "V1")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", f"s3a://{bucket()}/warehouse")
        .getOrCreate()
    )
    # INFO logs thousands of lines per job, which Airflow then has to capture and store.
    session.sparkContext.setLogLevel("WARN")
    return session


def merge_into(spark: SparkSession, df: DataFrame, table: str, key: str):
    """Idempotent upsert: replaying the same rows leaves the table unchanged."""
    view = f"merge_source_{table.replace('.', '_')}"
    df.createOrReplaceTempView(view)
    spark.sql(f"""
        MERGE INTO {CATALOG}.{table} AS target
        USING {view} AS source
        ON target.{key} = source.{key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def assert_unique(spark: SparkSession, table: str, key: str):
    """Fail the run if a merge left duplicate keys: downstream numbers would be wrong."""
    row = spark.sql(f"SELECT count(*) AS total, count(DISTINCT {key}) AS ids FROM {CATALOG}.{table}").first()
    if row.total != row.ids:
        raise ValueError(f"{table} has {row.total:,} rows but only {row.ids:,} distinct {key} values")
