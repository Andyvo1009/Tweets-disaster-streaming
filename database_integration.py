"""
Database Integration Solutions for Disaster Prediction Pipeline

This module provides multiple database integration options for storing
streaming disaster predictions from the Kafka + Spark + DistilBERT pipeline.

Supported databases:
1. PostgreSQL - Relational database with JSON support
2. MongoDB - Document database for flexible schema
3. InfluxDB - Time-series database for real-time analytics
4. SQLite - Local database for development/testing
5. Cassandra - Distributed NoSQL for high-volume data
"""

import os
import json
from datetime import datetime
from typing import Dict, Any, Optional
import logging

# Database connectors (install as needed)
try:
    import psycopg2
    import psycopg2.extras
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import pymongo
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False

try:
    import sqlite3
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

try:
    from influxdb_client import InfluxDBClient, Point
    from influxdb_client.client.write_api import SYNCHRONOUS
    INFLUX_AVAILABLE = True
except ImportError:
    INFLUX_AVAILABLE = False

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Database configuration settings"""
    
    # PostgreSQL
    POSTGRES_CONFIG = {
        'host': 'localhost',
        'port': 5432,
        'database': 'disaster_predictions',
        'user': 'your_username',
        'password': 'your_password'
    }
    
    # MongoDB
    MONGO_CONFIG = {
        'host': 'localhost',
        'port': 27017,
        'database': 'disaster_predictions',
        'collection': 'predictions'
    }
    
    # InfluxDB
    INFLUX_CONFIG = {
        'url': 'http://localhost:8086',
        'token': 'your_token_here',
        'org': 'your_org',
        'bucket': 'disaster_predictions'
    }
    
    # SQLite
    SQLITE_CONFIG = {
        'database': 'data/disaster_predictions.db'
    }
    
    # Cassandra
    CASSANDRA_CONFIG = {
        'hosts': ['localhost'],
        'port': 9042,
        'keyspace': 'disaster_predictions'
    }

class PostgreSQLConnector:
    """PostgreSQL database connector for disaster predictions"""
    
    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        
    def connect(self):
        """Establish PostgreSQL connection"""
        if not POSTGRES_AVAILABLE:
            raise ImportError("PostgreSQL dependencies not installed. Run: pip install psycopg2-binary")
        
        try:
            self.connection = psycopg2.connect(**self.config)
            logger.info("✅ Connected to PostgreSQL")
            self.create_tables()
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            raise
    
    def create_tables(self):
        """Create disaster predictions table"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS disaster_predictions (
            id SERIAL PRIMARY KEY,
            tweet_id VARCHAR(50),
            text TEXT NOT NULL,
            disaster_type VARCHAR(100),
            location VARCHAR(200),
            confidence DECIMAL(5,4),
            kafka_timestamp TIMESTAMP,
            processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_disaster_type ON disaster_predictions(disaster_type);
        CREATE INDEX IF NOT EXISTS idx_location ON disaster_predictions(location);
        CREATE INDEX IF NOT EXISTS idx_confidence ON disaster_predictions(confidence);
        CREATE INDEX IF NOT EXISTS idx_kafka_timestamp ON disaster_predictions(kafka_timestamp);
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_sql)
            self.connection.commit()
            logger.info("✅ PostgreSQL tables created/verified")
    
    def insert_prediction(self, data: dict):
        """Insert a single prediction"""
        insert_sql = """
        INSERT INTO disaster_predictions 
        (tweet_id, text, disaster_type, location, confidence, kafka_timestamp)
        VALUES (%(id)s, %(text)s, %(disaster)s, %(location)s, %(confidence)s, %(kafka_timestamp)s)
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_sql, data)
                self.connection.commit()
                logger.info(f"✅ Inserted prediction for tweet {data.get('id')}")
        except Exception as e:
            logger.error(f"❌ Failed to insert prediction: {e}")
            self.connection.rollback()
    
    def insert_batch(self, predictions: list):
        """Insert multiple predictions efficiently"""
        insert_sql = """
        INSERT INTO disaster_predictions 
        (tweet_id, text, disaster_type, location, confidence, kafka_timestamp)
        VALUES (%(id)s, %(text)s, %(disaster)s, %(location)s, %(confidence)s, %(kafka_timestamp)s)
        """
        
        try:
            with self.connection.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, insert_sql, predictions)
                self.connection.commit()
                logger.info(f"✅ Inserted batch of {len(predictions)} predictions")
        except Exception as e:
            logger.error(f"❌ Failed to insert batch: {e}")
            self.connection.rollback()
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("🔌 PostgreSQL connection closed")

class MongoDBConnector:
    """MongoDB connector for disaster predictions"""
    
    def __init__(self, config: dict):
        self.config = config
        self.client = None
        self.database = None
        self.collection = None
    
    def connect(self):
        """Establish MongoDB connection"""
        if not MONGO_AVAILABLE:
            raise ImportError("MongoDB dependencies not installed. Run: pip install pymongo")
        
        try:
            connection_string = f"mongodb://{self.config['host']}:{self.config['port']}"
            self.client = pymongo.MongoClient(connection_string)
            self.database = self.client[self.config['database']]
            self.collection = self.database[self.config['collection']]
            
            # Test connection
            self.client.admin.command('ping')
            logger.info("✅ Connected to MongoDB")
            self.create_indexes()
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            raise
    
    def create_indexes(self):
        """Create indexes for better query performance"""
        indexes = [
            ("disaster", 1),
            ("location", 1),
            ("confidence", -1),
            ("kafka_timestamp", -1),
            ("processing_timestamp", -1)
        ]
        
        for index in indexes:
            self.collection.create_index([index])
        logger.info("✅ MongoDB indexes created/verified")
    
    def insert_prediction(self, data: dict):
        """Insert a single prediction"""
        try:
            # Add processing timestamp
            data['processing_timestamp'] = datetime.now()
            
            result = self.collection.insert_one(data)
            logger.info(f"✅ Inserted prediction with ID: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            logger.error(f"❌ Failed to insert prediction: {e}")
    
    def insert_batch(self, predictions: list):
        """Insert multiple predictions"""
        try:
            # Add processing timestamps
            for pred in predictions:
                pred['processing_timestamp'] = datetime.now()
            
            result = self.collection.insert_many(predictions)
            logger.info(f"✅ Inserted batch of {len(result.inserted_ids)} predictions")
            return result.inserted_ids
        except Exception as e:
            logger.error(f"❌ Failed to insert batch: {e}")
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logger.info("🔌 MongoDB connection closed")

class SQLiteConnector:
    """SQLite connector for local development/testing"""
    
    def __init__(self, config: dict):
        self.config = config
        self.connection = None
        
        # Ensure data directory exists
        os.makedirs(os.path.dirname(config['database']), exist_ok=True)
    
    def connect(self):
        """Establish SQLite connection"""
        try:
            self.connection = sqlite3.connect(self.config['database'])
            self.connection.row_factory = sqlite3.Row  # Enable dict-like access
            logger.info(f"✅ Connected to SQLite: {self.config['database']}")
            self.create_tables()
        except Exception as e:
            logger.error(f"❌ SQLite connection failed: {e}")
            raise
    
    def create_tables(self):
        """Create disaster predictions table"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS disaster_predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tweet_id TEXT,
            text TEXT NOT NULL,
            disaster_type TEXT,
            location TEXT,
            confidence REAL,
            kafka_timestamp TEXT,
            processing_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_disaster_type ON disaster_predictions(disaster_type);
        CREATE INDEX IF NOT EXISTS idx_location ON disaster_predictions(location);
        CREATE INDEX IF NOT EXISTS idx_confidence ON disaster_predictions(confidence);
        """
        
        cursor = self.connection.cursor()
        cursor.executescript(create_table_sql)
        self.connection.commit()
        logger.info("✅ SQLite tables created/verified")
    
    def insert_prediction(self, data: dict):
        """Insert a single prediction"""
        insert_sql = """
        INSERT INTO disaster_predictions 
        (tweet_id, text, disaster_type, location, confidence, kafka_timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(insert_sql, (
                data.get('id'),
                data.get('text'),
                data.get('disaster'),
                data.get('location'),
                data.get('confidence'),
                data.get('kafka_timestamp')
            ))
            self.connection.commit()
            logger.info(f"✅ Inserted prediction for tweet {data.get('id')}")
        except Exception as e:
            logger.error(f"❌ Failed to insert prediction: {e}")
    
    def insert_batch(self, predictions: list):
        """Insert multiple predictions"""
        insert_sql = """
        INSERT INTO disaster_predictions 
        (tweet_id, text, disaster_type, location, confidence, kafka_timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        
        try:
            cursor = self.connection.cursor()
            data_tuples = [
                (p.get('id'), p.get('text'), p.get('disaster'), 
                 p.get('location'), p.get('confidence'), p.get('kafka_timestamp'))
                for p in predictions
            ]
            cursor.executemany(insert_sql, data_tuples)
            self.connection.commit()
            logger.info(f"✅ Inserted batch of {len(predictions)} predictions")
        except Exception as e:
            logger.error(f"❌ Failed to insert batch: {e}")
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("🔌 SQLite connection closed")

class DatabaseManager:
    """Unified database manager for multiple database types"""
    
    def __init__(self, db_type: str = "sqlite"):
        self.db_type = db_type.lower()
        self.connector = None
        
        # Initialize appropriate connector
        if self.db_type == "postgresql":
            self.connector = PostgreSQLConnector(DatabaseConfig.POSTGRES_CONFIG)
        elif self.db_type == "mongodb":
            self.connector = MongoDBConnector(DatabaseConfig.MONGO_CONFIG)
        elif self.db_type == "sqlite":
            self.connector = SQLiteConnector(DatabaseConfig.SQLITE_CONFIG)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def connect(self):
        """Connect to the database"""
        return self.connector.connect()
    
    def insert_prediction(self, data: dict):
        """Insert a single prediction"""
        return self.connector.insert_prediction(data)
    
    def insert_batch(self, predictions: list):
        """Insert multiple predictions"""
        return self.connector.insert_batch(predictions)
    
    def close(self):
        """Close database connection"""
        return self.connector.close()

# Example usage functions
def test_database_connection(db_type: str = "sqlite"):
    """Test database connection and insert sample data"""
    print(f"🧪 Testing {db_type.upper()} connection...")
    
    # Sample prediction data
    sample_data = {
        'id': 'test_001',
        'text': 'Earthquake hits the city causing major damage',
        'disaster': 'earthquake',
        'location': 'San Francisco',
        'confidence': 0.9567,
        'kafka_timestamp': datetime.now().isoformat()
    }
    
    try:
        # Create database manager
        db_manager = DatabaseManager(db_type)
        db_manager.connect()
        
        # Insert sample data
        db_manager.insert_prediction(sample_data)
        
        # Test batch insert
        batch_data = [
            {
                'id': 'test_002',
                'text': 'Flood warning issued for the area',
                'disaster': 'flood',
                'location': 'Houston',
                'confidence': 0.8934,
                'kafka_timestamp': datetime.now().isoformat()
            },
            {
                'id': 'test_003',
                'text': 'Wildfire spreading rapidly',
                'disaster': 'wildfire',
                'location': 'California',
                'confidence': 0.9123,
                'kafka_timestamp': datetime.now().isoformat()
            }
        ]
        
        db_manager.insert_batch(batch_data)
        
        print(f"✅ {db_type.upper()} test completed successfully!")
        
        # Close connection
        db_manager.close()
        
    except Exception as e:
        print(f"❌ {db_type.upper()} test failed: {e}")

if __name__ == "__main__":
    print("Database Integration Testing")
    print("=" * 40)
    
    # Test available databases
    available_dbs = ["sqlite"]
    
    if POSTGRES_AVAILABLE:
        available_dbs.append("postgresql")
    if MONGO_AVAILABLE:
        available_dbs.append("mongodb")
    
    for db_type in available_dbs:
        test_database_connection(db_type)
        print()
