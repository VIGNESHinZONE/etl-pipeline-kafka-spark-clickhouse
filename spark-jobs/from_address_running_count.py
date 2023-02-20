from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "tokens_kafka"

spark = SparkSession.builder.appName("read_test_straeam").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()


'from_address', 'to_address', 'block_timestamp', 'transaction_hash', 'log_index'

SCHEMA = StructType([
    StructField("from_address", StringType()),
    StructField("to_address", StringType()),
    StructField("block_timestamp", LongType()),
    StructField("transaction_hash", StringType()),
    StructField("log_index", LongType())
])

df_stream = df.select(
        F.from_json(F.decode(F.col("value"), "iso-8859-1"), SCHEMA).alias("value")
    ).select("value.*") \
    .select(
        F.col("from_address").alias("from_address"),
        F.col("to_address").alias("to_address"),
        F.col("block_timestamp").alias("block_timestamp"),
        F.from_unixtime(F.col("block_timestamp")).alias("datetime"),

        F.col("transaction_hash").alias("transaction_hash"),
        F.col("log_index").alias("log_index")
    ) \
    .withColumn("datetime", F.to_timestamp("datetime"))

df_stream_sent = df_stream.groupBy("from_address").agg(F.count("from_address").alias("from_count")).select(F.col("from_address").alias("address"), F.col("from_count"))
# df_stream_recieved = df_stream.groupBy("to_address").agg(F.count("to_address").alias("to_count")).select(F.col("to_address").alias("address"), F.col("to_count"))
# df_address = df_stream_sent.join(df_stream_recieved, df_stream_recieved.address == df_stream_sent.address)
 
def foreach_batch_function(df, epoch_id):
    
    df.write.parquet(f"streaming_output/sent_count/{epoch_id}.parquet", "overwrite")

  
df_stream_sent \
    .writeStream \
    .outputMode("Complete") \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()   
