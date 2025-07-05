"""
Database Setup and Testing Script for Disaster Prediction Pipeline

This script helps you:
1. Set up different database types
2. Create necessary tables and indexes
3. Test database connectivity
4. Configure database settings
5. View stored predictions
"""

import os
import sys
import json
from datetime import datetime
from database_integration import DatabaseManager, DatabaseConfig
from spark_database_writer import create_database_schema_sql

def setup_sqlite():
    """Set up SQLite database (easiest option for testing)"""
    print("🗄️  Setting up SQLite database...")
    
    try:
        # Ensure data directory exists
        os.makedirs("data", exist_ok=True)
        
        # Create database manager
        db_manager = DatabaseManager("sqlite")
        db_manager.connect()
        
        print("✅ SQLite database created successfully!")
        print(f"📍 Location: {DatabaseConfig.SQLITE_CONFIG['database']}")
        
        # Insert test data
        test_data = {
            'id': 'setup_test_001',
            'text': 'Test earthquake alert for database setup verification',
            'disaster': 'earthquake',
            'location': 'Test City',
            'confidence': 0.95,
            'kafka_timestamp': datetime.now().isoformat()
        }
        
        db_manager.insert_prediction(test_data)
        print("✅ Test data inserted successfully!")
        
        db_manager.close()
        return True
        
    except Exception as e:
        print(f"❌ SQLite setup failed: {e}")
        return False

def setup_postgresql():
    """Set up PostgreSQL database"""
    print("🐘 Setting up PostgreSQL database...")
    
    # Check if psycopg2 is installed
    try:
        import psycopg2
    except ImportError:
        print("❌ PostgreSQL dependencies not found!")
        print("Install with: pip install psycopg2-binary")
        return False
    
    print("📋 PostgreSQL Configuration:")
    print("   You need to update the configuration in database_integration.py")
    print("   Default config:")
    for key, value in DatabaseConfig.POSTGRES_CONFIG.items():
        print(f"   {key}: {value}")
    
    print("\n🔧 Setup Steps:")
    print("1. Install PostgreSQL server")
    print("2. Create database: CREATE DATABASE disaster_predictions;")
    print("3. Update credentials in DatabaseConfig.POSTGRES_CONFIG")
    print("4. Run this script again to test connection")
    
    try:
        db_manager = DatabaseManager("postgresql")
        db_manager.connect()
        print("✅ PostgreSQL connection successful!")
        db_manager.close()
        return True
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        return False

def setup_mongodb():
    """Set up MongoDB database"""
    print("🍃 Setting up MongoDB database...")
    
    # Check if pymongo is installed
    try:
        import pymongo
    except ImportError:
        print("❌ MongoDB dependencies not found!")
        print("Install with: pip install pymongo")
        return False
    
    try:
        db_manager = DatabaseManager("mongodb")
        db_manager.connect()
        print("✅ MongoDB connection successful!")
        
        # Insert test data
        test_data = {
            'id': 'mongo_test_001',
            'text': 'MongoDB test for disaster prediction pipeline',
            'disaster': 'flood',
            'location': 'Test Location',
            'confidence': 0.87,
            'kafka_timestamp': datetime.now().isoformat()
        }
        
        db_manager.insert_prediction(test_data)
        print("✅ Test data inserted successfully!")
        
        db_manager.close()
        return True
        
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        print("Make sure MongoDB server is running on localhost:27017")
        return False

def view_predictions(db_type="sqlite", limit=10):
    """View stored predictions from database"""
    print(f"👀 Viewing predictions from {db_type.upper()} database...")
    
    if db_type == "sqlite":
        try:
            import sqlite3
            db_path = DatabaseConfig.SQLITE_CONFIG['database']
            
            if not os.path.exists(db_path):
                print(f"❌ Database file not found: {db_path}")
                return
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT tweet_id, disaster_type, location, confidence, 
                       substr(text, 1, 50) as text_preview,
                       processing_timestamp
                FROM disaster_predictions 
                ORDER BY processing_timestamp DESC 
                LIMIT ?
            """, (limit,))
            
            rows = cursor.fetchall()
            
            if rows:
                print(f"\n📊 Latest {len(rows)} predictions:")
                print("-" * 120)
                print(f"{'ID':<15} {'Disaster':<12} {'Location':<15} {'Conf.':<6} {'Text Preview':<50} {'Timestamp':<20}")
                print("-" * 120)
                
                for row in rows:
                    tweet_id, disaster, location, confidence, text, timestamp = row
                    print(f"{str(tweet_id):<15} {str(disaster):<12} {str(location):<15} {confidence:<6.3f} {str(text):<50} {str(timestamp):<20}")
            else:
                print("📭 No predictions found in database")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Error viewing SQLite data: {e}")
    
    elif db_type == "mongodb":
        try:
            db_manager = DatabaseManager("mongodb")
            db_manager.connect()
            
            # Get recent predictions
            predictions = db_manager.connector.collection.find().sort("processing_timestamp", -1).limit(limit)
            
            predictions_list = list(predictions)
            
            if predictions_list:
                print(f"\n📊 Latest {len(predictions_list)} predictions:")
                print("-" * 120)
                
                for pred in predictions_list:
                    print(f"ID: {pred.get('id', 'N/A')}")
                    print(f"Disaster: {pred.get('disaster', 'N/A')}")
                    print(f"Location: {pred.get('location', 'N/A')}")
                    print(f"Confidence: {pred.get('confidence', 0):.3f}")
                    print(f"Text: {pred.get('text', '')[:50]}...")
                    print(f"Timestamp: {pred.get('processing_timestamp', 'N/A')}")
                    print("-" * 50)
            else:
                print("📭 No predictions found in database")
            
            db_manager.close()
            
        except Exception as e:
            print(f"❌ Error viewing MongoDB data: {e}")
    
    else:
        print(f"❌ Viewing data for {db_type} not implemented yet")

def generate_database_config():
    """Generate database configuration file"""
    print("⚙️  Generating database configuration...")
    
    config = {
        "database_settings": {
            "enabled": True,
            "type": "sqlite",  # Change to your preferred database
            "description": "Database configuration for disaster prediction pipeline"
        },
        "sqlite": DatabaseConfig.SQLITE_CONFIG,
        "postgresql": DatabaseConfig.POSTGRES_CONFIG,
        "mongodb": DatabaseConfig.MONGODB_CONFIG,
        "mysql": {
            "host": "localhost",
            "port": 3306,
            "database": "disaster_predictions",
            "user": "root",
            "password": "your_password"
        }
    }
    
    config_file = "database_config.json"
    
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"✅ Configuration saved to: {config_file}")
    print("📝 Edit this file to customize your database settings")

def test_all_databases():
    """Test all available database connections"""
    print("🧪 Testing all available databases...")
    
    results = {}
    
    # Test SQLite
    print("\n1️⃣ Testing SQLite...")
    results['sqlite'] = setup_sqlite()
    
    # Test PostgreSQL (if available)
    print("\n2️⃣ Testing PostgreSQL...")
    try:
        import psycopg2
        results['postgresql'] = setup_postgresql()
    except ImportError:
        print("⏭️ PostgreSQL dependencies not installed, skipping...")
        results['postgresql'] = False
    
    # Test MongoDB (if available)
    print("\n3️⃣ Testing MongoDB...")
    try:
        import pymongo
        results['mongodb'] = setup_mongodb()
    except ImportError:
        print("⏭️ MongoDB dependencies not installed, skipping...")
        results['mongodb'] = False
    
    # Summary
    print("\n📊 Database Test Results:")
    print("-" * 30)
    for db_type, success in results.items():
        status = "✅ Working" if success else "❌ Failed"
        print(f"{db_type.upper():<12}: {status}")
    
    working_dbs = [db for db, success in results.items() if success]
    if working_dbs:
        print(f"\n🎉 {len(working_dbs)} database(s) ready to use!")
        print("💡 Update DATABASE_TYPE in spark_bert_stream.py to use your preferred database")
    else:
        print("\n⚠️  No databases are working. Please check configurations.")

def main_menu():
    """Main menu for database setup"""
    while True:
        print("\n" + "="*50)
        print("🗄️  Database Setup for Disaster Prediction Pipeline")
        print("="*50)
        print("1. Set up SQLite (recommended for testing)")
        print("2. Set up PostgreSQL")
        print("3. Set up MongoDB")
        print("4. Test all databases")
        print("5. View predictions (SQLite)")
        print("6. View predictions (MongoDB)")
        print("7. Generate configuration file")
        print("8. Show database schemas")
        print("9. Exit")
        print("="*50)
        
        choice = input("Choose an option (1-9): ").strip()
        
        if choice == "1":
            setup_sqlite()
        elif choice == "2":
            setup_postgresql()
        elif choice == "3":
            setup_mongodb()
        elif choice == "4":
            test_all_databases()
        elif choice == "5":
            view_predictions("sqlite")
        elif choice == "6":
            view_predictions("mongodb")
        elif choice == "7":
            generate_database_config()
        elif choice == "8":
            schemas = create_database_schema_sql()
            for db_name, schema in schemas.items():
                print(f"\n{db_name.upper()} Schema:")
                print("-" * 40)
                print(schema)
        elif choice == "9":
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please try again.")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    print("Database Setup and Testing Tool")
    print("==============================")
    print("This tool helps you set up databases for storing disaster predictions")
    print("from your Kafka + Spark + DistilBERT pipeline.")
    print()
    
    main_menu()
