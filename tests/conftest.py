"""One local Spark session for every test module, since a JVM only loads its jars once."""

import glob
import os

import pytest


@pytest.fixture(scope="session")
def spark():
    import pyspark
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]").appName("streamcart-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
    )
    # The platform image bakes the Avro connector into Spark; a pip install of pyspark lacks it.
    if not glob.glob(os.path.join(os.getenv("SPARK_HOME", "/nonexistent"), "jars", "spark-avro_*.jar")):
        builder = builder.config("spark.jars.packages", f"org.apache.spark:spark-avro_2.12:{pyspark.__version__}")
    session = builder.getOrCreate()
    yield session
    session.stop()
