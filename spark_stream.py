from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col
import os
import sys

# Schema Definitions for Kafka Streaming Data
def get_kafka_message_schema():
    """
    Define schema for Kafka message data (CSV row structure)
    
    This matches the CSV structure: id, keyword, location, text
    
    Returns:
        StructType: Schema for parsing JSON messages from Kafka
    """
    
    schema = StructType([
        StructField("id", StringType(), nullable=True),           # Tweet ID as string from CSV
        StructField("keyword", StringType(), nullable=True),      # Disaster keyword - optional
        StructField("location", StringType(), nullable=True),     # Location - optional
        StructField("text", StringType(), nullable=False)         # Tweet text - required
    ])
    
    return schema

def get_processed_message_schema():
    """
    Define schema for processed messages with predictions
    
    Returns:
        StructType: Schema for final output with predictions
    """
    
    schema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("keyword", StringType(), nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("text", StringType(), nullable=False),
        StructField("prediction", IntegerType(), nullable=True),       # 0 = safe, 1 = disaster
        StructField("confidence", StringType(), nullable=True),        # Confidence score
        StructField("disaster_label", StringType(), nullable=True),    # "DISASTER" or "NOT_DISASTER"
        StructField("timestamp", StringType(), nullable=True)          # Processing timestamp
    ])
    
    return schema


# Create directories if they don't exist


# Create Spark session WITHOUT the kafka package in the config
# We'll use spark-submit to provide that instead
spark = SparkSession.builder \
    .appName("KafkaToFile") \
    .master("local[*]") \
    .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")\
                     .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Created Spark session, connecting to Kafka...")

# Print schema information


try:
    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test-tweets") \
        .option("startingOffsets", "earliest") \
        .load()
        
    print("Connected to Kafka, processing messages...")

    # Get the schema for parsing JSON messages
    message_schema = get_kafka_message_schema()
    
    # Extract and parse the message content
    parsed_messages = df.select(
        # Parse the JSON value from Kafka
        from_json(col("value").cast("string"), message_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        # Extract fields from the parsed JSON
        col("data.id").alias("id"),
        col("data.keyword").alias("keyword"), 
        col("data.location").alias("location"),
        col("data.text").alias("text"),
        col("kafka_timestamp")
    ).filter(
        # Filter out messages with empty text
        col("text").isNotNull() & (col("text") != "")
    )
    
    # Write to console first
    print("Writing parsed messages to console...")
    console_query = parsed_messages.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .start()
    
    # Let console run for a bit 
    console_query.awaitTermination(10)
    print("Console output completed, now writing to file...")
    
    # Write to file with structured format
    file_query = parsed_messages.writeStream \
        .format("json") \
        .option("path", "output/stream") \
        .option("checkpointLocation", "output/checkpoint") \
        .outputMode("append") \
        .start()
        
    print("File output started, awaiting termination...")
    file_query.awaitTermination()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()