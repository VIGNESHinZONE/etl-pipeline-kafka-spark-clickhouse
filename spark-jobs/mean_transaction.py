from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "blocks_kafka"

spark = SparkSession.builder.appName("read_test_straeam").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

SCHEMA = StructType([
    StructField("timestamp", LongType()),
    StructField("number", LongType()),
    StructField("transaction_count", LongType()),
    StructField("gas_used", LongType())
])

df_stream = df.select(
        F.from_json(F.decode(F.col("value"), "iso-8859-1"), SCHEMA).alias("value")
    ).select("value.*") \
    .select(
        F.col("timestamp").alias("timestamp"),
        F.from_unixtime(F.col("timestamp")).alias("datetime"),
        F.col("number").alias("number"),
        F.col("transaction_count").alias("transaction_count"),
        F.col("gas_used").alias("gas_used")
    ) \
    .withColumn("input_timestamp", F.to_timestamp("datetime"))
# max_block_number = df.select(F.max(F.col("number"))).collect()[0]


df_stream\
    .groupBy(
        F.window("input_timestamp", "60 seconds")
    )\
    .agg(
        F.count("number").alias("total_blocks"),
        F.min("number").alias("min_block_number"),
        F.max("number").alias("max_block_number"),
        F.sum("transaction_count").alias("total_transactions"),
        F.mean("transaction_count").alias("mean_transactions")
    )\
    .select(F.col("total_blocks"), F.col("min_block_number"), F.col("max_block_number"), F.col("total_transactions"), F.col("mean_transactions"))\
    .writeStream\
    .option("truncate", "false")\
    .outputMode("update")\
    .format("console")\
    .start()\
    .awaitTermination()
