from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define the schema for the JSON data
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", StringType(), True),  # Read as StringType initially
    StructField("timestamp", StringType(), True)
])

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Read the stream from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "crypto-prices") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize the JSON data
json_df = raw_df.selectExpr("CAST(value AS STRING) as json")
data_df = json_df.withColumn("data", from_json(col("json"), schema))

# Select individual fields and convert price to DoubleType
final_df = data_df.select(
    col("data.symbol").alias("symbol"),
    col("data.price").cast(DoubleType()).alias("price"),
    from_unixtime(col("data.timestamp").cast("long") / 1000).alias("timestamp"))

# Start the query and write the stream to console (for debugging)
query = final_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()