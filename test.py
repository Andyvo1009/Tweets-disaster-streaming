"""
test_spark_udf.py

Test the DistilBERT model in Spark UDF context (like your streaming pipeline)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import pandas as pd
import os,sys
# Import your model classes
from bert_model import DisasterPredictor, extract_location_disaster
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
def test_spark_udf():
    """Test DistilBERT model in Spark UDF environment"""
    print("⚡ Testing DistilBERT model in Spark UDF context")
    print("=" * 60)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("DistilBERTUDFTest") \
        .master("local[2]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Initialize predictor
        predictor = DisasterPredictor()
        
        def predict_disaster_udf(text):
            """UDF wrapper for disaster prediction"""
            if text is None or text.strip() == "":
                return "0,0.0"
            
            prediction, confidence = predictor.predict_disaster(text)
            return f"{prediction},{confidence:.4f}"
        
        # Register UDFs
        predict_udf = udf(predict_disaster_udf, StringType())
        extractor_udf = udf(extract_location_disaster, StringType())
        
        # Create test data
        test_data = [
            {"id": "1", "text": "Earthquake hits California"},
            {"id": "2", "text": "Beautiful day at the park"},
            {"id": "3", "text": "Flood warning issued"},
            {"id": "4", "text": "Going to the movies"},
            {"id": "5", "text": "Wildfire spreads rapidly"}
        ]
        
        # Create DataFrame
        df = spark.createDataFrame(test_data)
        
        print("📊 Test Data:")
        df.show(truncate=False)
        
        # Apply UDF
        print("\n🔄 Applying prediction UDF...")
        predictions_df = df.withColumn("prediction_result", predict_udf(col("text")))
        
        # Split results
        from pyspark.sql.functions import split
        final_df = predictions_df \
            .withColumn("prediction", split(col("prediction_result"), ",")[0].cast("int")) \
            .withColumn("confidence", split(col("prediction_result"), ",")[1].cast("double")) \
            .drop("prediction_result")
        
        print("✅ Prediction Results:")
        final_df.show(truncate=False)
        
        # Apply location extractor for disasters only
        disasters_df = final_df.filter(col("prediction") == 1)
        
        if disasters_df.count() > 0:
            print("\n🔄 Applying location extraction UDF...")
            location_df = disasters_df.withColumn("location_info", extractor_udf(col("text")))
            
            print("🌍 Location Extraction Results:")
            location_df.show(truncate=False)
        else:
            print("\n📝 No disasters detected in test data")
        
        print("\n✅ Spark UDF test completed successfully!")
        
    except Exception as e:
        print(f"❌ Error in Spark UDF test: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()

if __name__ == "__main__":
    test_spark_udf()