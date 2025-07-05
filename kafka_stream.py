import json
import csv
import time
from kafka import KafkaProducer
import os

# Kafka Configuration Settings
KAFKA_CONFIG = {
    'KAFKA_BROKER_ID': 1,
    'KAFKA_ZOOKEEPER_CONNECT': 'zookeeper:2181',
    'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT',
    'KAFKA_ADVERTISED_LISTENERS': 'PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092',
    'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': 1,
    'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': 0,
    'KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR': 1,
    'KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR': 1,
    'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': 1,
    'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': 1
}

# Set environment variables for Kafka configuration
for key, value in KAFKA_CONFIG.items():
    os.environ[key] = str(value)

# Determine broker address based on environment
# Try Docker internal first, fallback to localhost
KAFKA_BROKERS = [
    "broker:29092",     # Docker internal
    "localhost:9092"    # Local development
]

def create_producer():
    """Create Kafka producer with retry logic for different brokers"""
    for broker in KAFKA_BROKERS:
        try:
            print(f"🔄 Attempting to connect to Kafka at: {broker}")
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"),
                retries=3,
                request_timeout_ms=30000,
                max_block_ms=30000
            )
            print(f"✅ Successfully connected to Kafka at: {broker}")
            return producer, broker
        except Exception as e:
            print(f"❌ Failed to connect to {broker}: {e}")
            continue
    
    raise Exception("❌ Could not connect to any Kafka broker")

# Create producer with connection retry
producer, connected_broker = create_producer()

folder = "data"
topic = "test-tweets"  # Updated topic name for disaster prediction
csv_file = os.path.join(folder, "test.csv")

print(f"📊 Kafka Configuration Summary:")
print(f"   Connected Broker: {connected_broker}")
print(f"   Topic: {topic}")
print(f"   CSV File: {csv_file}")
print(f"   Environment Variables Set: {len(KAFKA_CONFIG)} configs")
print("=" * 60)

# Process the CSV file
with open(csv_file, "r", encoding="utf-8") as f:
    csv_reader = csv.DictReader(f)
    row_count = 0
    
    print("🚀 Starting to stream data to Kafka...")
    
    for row in csv_reader:
        try:
            # Send each row as a message
            producer.send(topic, value=row)
            row_count += 1
            
            # Print confirmation with the text field
            text_preview = row.get("text", "")[:60] + "..." if len(row.get("text", "")) > 60 else row.get("text", "")
            print(f"✅ Sent {row_count}: {text_preview}")
            
            # Small delay to simulate streaming
            time.sleep(10)
            
        except Exception as e:
            print(f"❌ Error sending row {row_count}: {e}")

print(f"\n📈 Streaming completed!")
print(f"   Total messages sent: {row_count}")
print(f"   Broker used: {connected_broker}")
print(f"   Topic: {topic}")

# Make sure all messages are sent
print("🔄 Flushing remaining messages...")
producer.flush()
print("✅ All messages sent successfully!")

# Close producer
producer.close()
print("🔌 Producer connection closed.")