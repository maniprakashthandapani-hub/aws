import os
import sys
import random
from datetime import datetime, timedelta
from pymongo import MongoClient

def upload_sample_data(num_records=5000):
    # Fetch Mongo URI from env
    mongo_uri = os.environ.get("MONGO_URI")
    if not mongo_uri:
        print("ERROR: MONGO_URI environment variable is not set!")
        print("Please run: export MONGO_URI='your-mongodb-connection-string'")
        sys.exit(1)
        
    print("Connecting to MongoDB Atlas...")
    client = MongoClient(mongo_uri)
    db = client["data_lake_db"]
    collection = db["customer_transactions"]
    
    # Clean previous data to ensure clean runs
    print("Cleaning existing collection...")
    collection.delete_many({})
    
    print(f"Generating {num_records} sample documents...")
    batch = []
    start_date = datetime(2025, 1, 1)
    
    for i in range(num_records):
        c_id = random.randint(10000, 99999)
        name = f"User_{random.randint(1000, 9999)}"
        email = f"user_{random.randint(1000, 9999)}@example.com"
        phone = f"+447700900{random.randint(100, 999)}"
        t_date = (start_date + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
        amount = round(random.uniform(10.0, 5000.0), 2)
        
        # Introduce a few bad documents (1% failure rate for DQ test validation)
        if random.random() < 0.01:
            amount = -50.0  # Fails RQ_002 (range check)
        if random.random() < 0.01:
            email = "corrupted_email_structure" # Fails RQ_004 (regex check)
            
        doc = {
            "customer_id": c_id,
            "name": name,
            "email": email,
            "phone": phone,
            "transaction_date": t_date,
            "amount": amount
        }
        batch.append(doc)
        
        # Write in batches of 1000
        if len(batch) >= 1000:
            collection.insert_many(batch)
            print(f"Uploaded {i + 1} documents...")
            batch = []
            
    if batch:
        collection.insert_many(batch)
        print(f"Uploaded {num_records} documents total.")
        
    print("Sample data successfully loaded into MongoDB Atlas!")

if __name__ == "__main__":
    records = 5000
    if len(sys.argv) > 1:
        try:
            records = int(sys.argv[1])
        except ValueError:
            pass
    upload_sample_data(records)
