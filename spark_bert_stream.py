"""
Spark Streaming Script for Disaster Prediction using Custom Trained DistilBERT Model


Real-time streaming from Kafka topic

The script uses your trained DistilBERT model (located in distilBERT/ folder) 
to predict whether text is about a disaster. The model uses PEFT (LoRA) 
adapters trained on your custom dataset.

Kafka Integration:
- Consumes from topic: test-tweets
- Expects JSON format: {"id": "...", "keyword": "...", "location": "...", "text": "..."}
- Outputs predictions with confidence scores
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, monotonically_increasing_id
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
import os
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from peft import PeftModel
import pandas as pd
import sys

import warnings
import traceback

from bert_model import *

warnings.filterwarnings("ignore")
print(sys.executable)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Database configuration - Change these settings as needed
DATABASE_ENABLED = False  # Set to False to disable database writes
DATABASE_TYPE = "sqlite"  # Options: "sqlite", "postgresql", "mysql", "mongodb"
# Schema Definitions for Disaster Tweet Data





# Create directories if they don't exist
os.makedirs("output/stream", exist_ok=True)
os.makedirs("output/checkpoint", exist_ok=True)
os.makedirs("output/predictions", exist_ok=True)

# Initialize the predictor (singleton pattern)
predictor = None

def get_predictor():
    global predictor
    if predictor is None:
        predictor = DisasterPredictor()
    return predictor

def predict_disaster_udf(text):
    """UDF wrapper for disaster prediction"""
    if text is None or text.strip() == "":
        return "0,0.0"  # Return as string to be parsed later
    
    pred = get_predictor()
    prediction, confidence = pred.predict_disaster(text)
    return f"{prediction},{confidence:.4f}"

# Register UDF
predict_udf = udf(predict_disaster_udf, StringType())
extractor_udf=udf(extract_location_disaster,StringType())
def create_spark_session():
    """Create Spark session with appropriate configuration"""
    return SparkSession.builder \
        .appName("CSVDistilBERTDisasterPrediction") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .config("spark.python.worker.faulthandler.enabled", "true") \
        .getOrCreate()


def process_kafka_stream():
    """Process real-time data from Kafka stream with BERT predictions"""
    global DATABASE_ENABLED
    # Create Spark session with Kafka support
    spark = SparkSession.builder \
    .appName("KafkaDistilBERTDisasterPrediction") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.driver.memory", "6g").config("spark.executor.memory", "4g")\
    .config("spark.python.worker.memory", "1g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
    .config("spark.python.worker.faulthandler.enabled", "true") \
    .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    print("🚀 Starting Kafka streaming with DistilBERT disaster prediction...")
    print("📊 Configuration:")
    print("   - Topic: test-tweets")
    print("   - Brokers: localhost:9092, broker:29092")
    print("   - Model: distilBERT/ (DistilBERT + LoRA adapters)")
    print("   - Output: Console + Files")
    print("=" * 60)
    
    try:
        # Define Kafka brokers (try multiple options)
        kafka_brokers = "localhost:9092"  # Primary broker
        
        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_brokers) \
            .option("subscribe", "test-tweets") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("✅ Connected to Kafka stream successfully!")
        
        # Parse JSON messages from Kafka
        from pyspark.sql.functions import from_json, col, current_timestamp
        
        # Define schema for the JSON messages from kafka_stream.py
        json_schema = StructType([
            StructField("id", StringType(), nullable=True),        # ID as string from CSV
            StructField("keyword", StringType(), nullable=True),   # Keyword field
            StructField("location", StringType(), nullable=True),  # Location field  
            StructField("text", StringType(), nullable=False)      # Text content
        ])
        
        # Parse the JSON value from Kafka
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), json_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Filter out null text values
        filtered_df = parsed_df.filter(col("text").isNotNull() & (col("text") != ""))
        
        # Add DistilBERT predictions using UDF
        predictions_df = filtered_df.withColumn("prediction_result", predict_udf(col("text"))).drop("location").drop("keyword")
        
        
        # Split prediction results into separate columns
        from pyspark.sql.functions import split, when
        final_df = predictions_df \
            .withColumn("prediction", split(col("prediction_result"), ",")[0].cast(IntegerType())) \
            .withColumn("confidence", split(col("prediction_result"), ",")[1].cast("double")) \
            .drop("prediction_result")
        final_df = final_df.filter(col("prediction") == 1)
        final_df = final_df.withColumn(
            "location_disaster",
            extractor_udf(col("text"))  # Extract location and disaster info
        )
        disaster_schema = StructType() \
    .add("location", StringType()) \
    .add("disaster", StringType())
        final_df = final_df.withColumn("json_disaster", from_json(col("location_disaster"), disaster_schema))\
                .withColumn("location", col("json_disaster.location")) \
              .withColumn("disaster", col("json_disaster.disaster")) \
              .drop("json_disaster")
        # Select final columns for output
        output_df = final_df.select(
            "id", "text", "disaster", "location", 
            "confidence",
            "kafka_timestamp"
        )
        
        print("🔄 Starting streaming query...")
        
        # Database setup
        db_writer = None
        
        
        # Write to console (for monitoring)
        console_query = output_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 10) \
            .start()
        
        # Write to files for persistence
        file_query = output_df.writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", "output/kafka_predictions") \
            .option("checkpointLocation", "output/checkpoint/kafka_stream") \
            .trigger(processingTime="20 seconds") \
            .start()
        
        # Write to database (if enabled)
        db_query = None
        if DATABASE_ENABLED and db_writer:
            db_query = output_df.writeStream \
                .outputMode("append") \
                .foreachBatch(db_writer) \
                .option("checkpointLocation", "output/checkpoint/database_stream") \
                .trigger(processingTime="15 seconds") \
                .start()
            
            print(f"✅ Database streaming enabled: {DATABASE_TYPE.upper()}")
        
        print("✅ Streaming queries started!")
        print("📊 Monitoring:")
        print("   - Console output: Live predictions displayed")
        print("   - File output: output/kafka_predictions/")
        if DATABASE_ENABLED:
            print(f"   - Database output: {DATABASE_TYPE.upper()} database")
        print("   - Checkpoint: output/checkpoint/kafka_stream/")
        print("\n🔍 Waiting for messages from Kafka topic 'test-tweets'...")
        print("💡 Run kafka_stream.py to send data to this stream")
        print("🛑 Press Ctrl+C to stop streaming")
        print("=" * 60)
        
        # Wait for termination
        try:
            console_query.awaitTermination()
            file_query.awaitTermination()
            if db_query:
                db_query.awaitTermination()
        except KeyboardInterrupt:
            print("\n🛑 Stopping all streaming queries...")
            console_query.stop()
            file_query.stop()
            if db_query:
                db_query.stop()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping Kafka stream...")
    except Exception as e:
        print(f"❌ Error in Kafka streaming: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("✅ Spark session closed.")

if __name__ == "__main__":
    # print("DistilBERT Disaster Prediction with Spark")
    # print("==========================================")
    # print("Using YOUR trained DistilBERT model from distilBERT/ folder")
    # print("Model: DistilBERT-base-uncased + LoRA adapters")
    # print("Task: Binary classification (Disaster vs Non-disaster)")
   
    
    # Choose processing mode
    process_kafka_stream()