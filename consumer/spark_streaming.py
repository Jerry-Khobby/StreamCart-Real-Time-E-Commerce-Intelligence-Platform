import os 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import (
  col,
  from_json,
  to_timestamp,
  year,
  month,
  dayofmonth
) 
from pyspark.sql.types import StructType,StringType,DoubleType, IntegerType, BooleanType 


#Create a spark session 
def create_spark_session():
    return (
        SparkSession.builder
        .appName("StreamCart-Bronze-Layer")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints")
        
        # MinIO (S3) configs
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    
    

#Schema(Bronze = still structured but raw-ish)
transaction_schema = StructType() \
    .add("event_type", StringType()) \
    .add("transaction_id", StringType()) \
    .add("user_id", StringType()) \
    .add("product_id", StringType()) \
    .add("category", StringType()) \
    .add("region", StringType()) \
    .add("currency", StringType()) \
    .add("amount_local", DoubleType()) \
    .add("amount_usd", DoubleType()) \
    .add("quantity", IntegerType()) \
    .add("payment_method", StringType()) \
    .add("status", StringType()) \
    .add("timestamp", StringType()) \
    .add("_is_fraud", BooleanType()) \
    .add("_fraud_type", StringType())
    
    

clickstream_schema = StructType() \
    .add("event_type", StringType()) \
    .add("event_id", StringType()) \
    .add("session_id", StringType()) \
    .add("user_id", StringType()) \
    .add("region", StringType()) \
    .add("page_type", StringType()) \
    .add("product_id", StringType()) \
    .add("search_query", StringType()) \
    .add("referrer", StringType()) \
    .add("user_agent", StringType()) \
    .add("timestamp", StringType()) \
    .add("_is_bot", BooleanType()) \
    .add("_session_age_s", DoubleType())
    
    

inventory_schema = StructType() \
    .add("event_type", StringType()) \
    .add("event_id", StringType()) \
    .add("product_id", StringType()) \
    .add("warehouse_id", StringType()) \
    .add("region", StringType()) \
    .add("update_type", StringType()) \
    .add("quantity_delta", IntegerType()) \
    .add("timestamp", StringType())
    
    

#subscribe to a kafka topic 
def read_kafka_stream(spark):
  return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "transactions,clickstream,inventory_updates")
        .option("startingOffsets", "latest")
        .load()
  )
  
  

#Parse + Route Streams 
def process_streams(df):
    json_df = df.selectExpr("CAST(value AS STRING) as json_str")

    # ---------------- TRANSACTIONS ----------------
    tx = (
        json_df
        .select(from_json(col("json_str"), transaction_schema).alias("data"))
        .select("data.*")
        .filter(col("event_type") == "transaction")
        .withColumn("event_time", to_timestamp("timestamp"))
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("day", dayofmonth("event_time"))
    )

    # ---------------- CLICKSTREAM ----------------
    click = (
        json_df
        .select(from_json(col("json_str"), clickstream_schema).alias("data"))
        .select("data.*")
        .filter(col("event_type") == "clickstream")
        .withColumn("event_time", to_timestamp("timestamp"))
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("day", dayofmonth("event_time"))
    )

    # ---------------- INVENTORY ----------------
    inventory = (
        json_df
        .select(from_json(col("json_str"), inventory_schema).alias("data"))
        .select("data.*")
        .filter(col("event_type") == "inventory_update")
        .withColumn("event_time", to_timestamp("timestamp"))
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("day", dayofmonth("event_time"))
    )

    return tx, click, inventory 
  
  

#I need to write minio 
def write_to_minio(df, path, checkpoint):
    return (
        df.writeStream
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .partitionBy("region", "year", "month", "day")
        .outputMode("append")
        .start()
    )
    
    
def main():
    spark = create_spark_session()

    kafka_df = read_kafka_stream(spark)

    tx_df, click_df, inventory_df = process_streams(kafka_df)

    bucket = os.getenv("MINIO_BUCKET", "streamcart")

    tx_query = write_to_minio(
        tx_df,
        f"s3a://{bucket}/bronze/transactions",
        "/tmp/checkpoints/transactions"
    )

    click_query = write_to_minio(
        click_df,
        f"s3a://{bucket}/bronze/clickstream",
        "/tmp/checkpoints/clickstream"
    )

    inventory_query = write_to_minio(
        inventory_df,
        f"s3a://{bucket}/bronze/inventory",
        "/tmp/checkpoints/inventory"
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()