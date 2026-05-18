import sys
import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description="EMR Serverless MongoDB Atlas IAM Connectivity Test")
    parser.add_argument("--mongo_uri", required=True, help="MongoDB connection URI with IAM auth settings")
    args = parser.parse_args()

    print("Initializing Spark Session with MongoDB configurations...")
    spark = SparkSession.builder \
        .appName("EMR-Mongo-IAM-Connectivity-Test") \
        .getOrCreate()

    print(f"Attempting passwordless IAM connection to collection: data_lake_db.customer_transactions")
    print(f"Target connection URI: {args.mongo_uri}")
    
    try:
        # Load from MongoDB using the official Spark MongoDB connector
        df = spark.read.format("mongodb") \
            .option("spark.mongodb.read.connection.uri", args.mongo_uri) \
            .option("spark.mongodb.read.database", "data_lake_db") \
            .option("spark.mongodb.read.collection", "customer_transactions") \
            .load()
            
        print("Successfully connected! Displaying collection schema:")
        df.printSchema()
        
        count = df.count()
        print(f"Total documents found in collection: {count}")
        
        if count > 0:
            print("Successfully loaded 3 sample documents:")
            df.show(3, truncate=False)
        else:
            print("Warning: Connection succeeded but the collection is empty. Run upload_mongo_sample.py first.")
            
    except Exception as e:
        print("ERROR: Connection failed!", file=sys.stderr)
        print(str(e), file=sys.stderr)
        sys.exit(1)
        
    finally:
        spark.stop()
        print("Spark Session stopped cleanly.")

if __name__ == "__main__":
    main()
