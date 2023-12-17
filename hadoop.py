from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace
from pyspark.sql.types import StructType, StringType, FloatType
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
# Create a Spark session
spark = SparkSession.builder.appName("CreditCardProcessing").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define the schema for the streaming data
schema = StructType().add("User", StringType())\
                     .add("Card", StringType())\
                     .add("Year", StringType())\
                     .add("Month", StringType())\
                     .add("Day", StringType())\
                     .add("Time", StringType())\
                     .add("Amount", StringType())\
                     .add("Use Chip", StringType())\
                     .add("Merchant Name", StringType())\
                     .add("Merchant City", StringType())\
                     .add("Merchant State", StringType())\
                     .add("Zip", StringType())\
                     .add("MCC", StringType())\
                     .add("Errors?", StringType())\
                     .add("Is Fraud?", StringType())

# Read data from Kafka using the structured streaming API
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .load()
# Convert the value from Kafka into a string
value_str = kafka_stream_df.selectExpr("CAST(value AS STRING)").alias("value")

# Parse the JSON string into a DataFrame
parsed_df = value_str.select(from_json("value", schema).alias("data")).select("data.*")
# Filter for records where 'Is Fraud?' is 'No'
non_fraud_df = parsed_df.filter("`Is Fraud?` = 'No'")

# Chuyển đổi kiểu dữ liệu cột "Amount"
non_fraud_df = non_fraud_df.withColumn("Amount", regexp_replace("Amount", "\\$", "").cast(FloatType()))
"""
# Write the processed DataFrame to HDFS in CSV format
hdfs_output_path = "hdfs://localhost:9000/myfolder"
query = non_fraud_df \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("checkpointLocation", "D:/") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .start(hdfs_output_path)
"""
#Đẩy dữ liệu lên hadoop
query = non_fraud_df \
    .writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "D:/") \
    .foreachBatch(lambda batch_df, batch_id: batch_df.coalesce(1).write.csv("hdfs://localhost:9000/myfolder/card", mode="append", header=True)) \
    .start()

#In ra terminal check
query = non_fraud_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()
