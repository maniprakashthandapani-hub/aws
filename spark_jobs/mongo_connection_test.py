import sys
import argparse
import urllib.parse
import boto3
from pyspark.sql import SparkSession

def get_assumed_mongo_uri(base_uri, role_arn):
    parsed = urllib.parse.urlparse(base_uri)
    base_cluster_url = parsed.netloc
    database_name = parsed.path.strip("/") or "data_lake_db"
    
    print(f"Assuming role {role_arn} for MongoDB access...")
    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName="EMRSessionMongoAccess"
    )
    credentials = assumed_role_object['Credentials']
    
    access_key = credentials['AccessKeyId']
    secret_key = credentials['SecretAccessKey']
    session_token = credentials['SessionToken']
    
    encoded_secret = urllib.parse.quote_plus(secret_key)
    encoded_token = urllib.parse.quote_plus(session_token)
    
    mongo_uri = (
        f"mongodb+srv://{access_key}:{encoded_secret}@{base_cluster_url}/{database_name}"
        f"?authSource=%24external&authMechanism=MONGODB-AWS"
        f"&authMechanismProperties=AWS_SESSION_TOKEN:{encoded_token}"
    )
    return mongo_uri

def main():
    parser = argparse.ArgumentParser(description="EMR Serverless MongoDB Atlas IAM Connectivity Test")
    parser.add_argument("--mongo_uri", required=True, help="MongoDB connection URI with IAM auth settings")
    parser.add_argument("--mongo_role_arn", required=False, default=None, help="Optional IAM role ARN to assume for MongoDB access")
    args = parser.parse_args()

    if args.mongo_role_arn:
        try:
            args.mongo_uri = get_assumed_mongo_uri(args.mongo_uri, args.mongo_role_arn)
        except Exception as e:
            print(f"ERROR: Failed to assume IAM role {args.mongo_role_arn}!", file=sys.stderr)
            print(str(e), file=sys.stderr)
            sys.exit(1)

    print("Initializing Spark Session with MongoDB configurations...")
    spark = SparkSession.builder \
        .appName("EMR-Mongo-IAM-Connectivity-Test") \
        .getOrCreate()

    print(f"Attempting passwordless IAM connection to collection: data_lake_db.customer_transactions")
    
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
