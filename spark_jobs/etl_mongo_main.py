import sys
import argparse
from schema_validator import SchemaValidator
from data_quality import DataQualityEngine
from encryption_utils import KMSEnvelopeEncryption
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, coalesce, current_timestamp
import json
import boto3
import urllib.parse

def get_assumed_mongo_uri(base_uri, role_arn):
    parsed = urllib.parse.urlparse(base_uri)
    base_cluster_url = parsed.netloc
    database_name = parsed.path.strip("/") or "data_lake_db"
    
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

def create_spark_session(app_name="DataPipelineMongoETL"):
    """Initializes and returns a SparkSession configured for MongoDB."""
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def standardise_dates(df, date_col):
    """
    Attempts to parse multiple date formats and standardize to yyyy-MM-dd.
    Uses coalesce to try formats in priority order.
    """
    return df.withColumn(
        date_col,
        coalesce(
            to_date(col(date_col), "yyyy-MM-dd"),
            to_date(col(date_col), "MM/dd/yyyy"),
            to_date(col(date_col), "dd-MM-yyyy")
        )
    )

def save_ciphertext_key_to_s3(ciphertext_blob, s3_path, dt, kms_key_arn):
    """
    Saves the KMS encrypted data key to S3 so consumers can decrypt the data later.
    """
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]
    bucket = s3_path.split("/")[0]
    prefix = "/".join(s3_path.split("/")[1:]).strip("/")
    
    file_key = f"{prefix}/dt={dt}/data_key.json"
    
    s3_client = boto3.client('s3')
    payload = json.dumps({"execution_date": dt, "encrypted_data_key_base64": ciphertext_blob})
    
    s3_client.put_object(
        Bucket=bucket, 
        Key=file_key, 
        Body=payload,
        ServerSideEncryption='aws:kms',
        SSEKMSKeyId=kms_key_arn
    )

def main():
    parser = argparse.ArgumentParser(description="PySpark MongoDB to S3 ETL Job")
    parser.add_argument("--mongo_uri", required=True, help="MongoDB Atlas connection URI with IAM auth")
    parser.add_argument("--mongo_role_arn", required=False, default=None, help="Optional IAM role ARN to assume for MongoDB access")
    parser.add_argument("--output_path", required=True, help="S3 path to output Parquet")
    parser.add_argument("--schema_path", required=True, help="S3 or local path to schema.json")
    parser.add_argument("--dq_rules_path", required=True, help="S3 or local path to dq_rules.json")
    parser.add_argument("--rejected_path", required=True, help="S3 path to write rejected rows")
    parser.add_argument("--kms_key_arn", required=True, help="ARN of the AWS KMS Key for column encryption")
    parser.add_argument("--keys_output_path", required=True, help="S3 path to save the encrypted data keys")
    parser.add_argument("--execution_date", required=True, help="Date partition (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.mongo_role_arn:
        try:
            args.mongo_uri = get_assumed_mongo_uri(args.mongo_uri, args.mongo_role_arn)
        except Exception as e:
            print(f"ERROR: Failed to assume IAM role {args.mongo_role_arn}!", file=sys.stderr)
            print(str(e), file=sys.stderr)
            sys.exit(1)

    spark = create_spark_session()
    logger = spark._jvm.org.apache.log4j.LogManager.getLogger("com.datapipeline.etl")
    logger.info(f"Starting MongoDB ETL for execution date: {args.execution_date}")

    # 1. Load Contract & Get Schema
    validator = SchemaValidator(args.schema_path)
    spark_schema = validator.get_pyspark_schema()

    # 2. Read from MongoDB
    logger.info("Reading transactional documents from MongoDB Atlas...")
    df = spark.read.format("mongodb") \
        .option("spark.mongodb.read.connection.uri", args.mongo_uri) \
        .option("spark.mongodb.read.database", "data_lake_db") \
        .option("spark.mongodb.read.collection", "customer_transactions") \
        .schema(spark_schema) \
        .load()

    # 3. Validate & Standardise (Null strategies)
    df = validator.validate_and_standardise(df)

    # 4. Standardise Dates
    df = standardise_dates(df, "transaction_date")

    # 5. Metadata tracking
    df = df.withColumn("etl_processed_at", current_timestamp())

    # 6. Data Quality Checks (Phase 3)
    logger.info("Applying Data Quality Rules")
    dq_engine = DataQualityEngine(args.dq_rules_path)
    valid_df, rejected_df = dq_engine.apply_rules(df)

    # 7. SPII Encryption (Phase 4)
    # MUST happen AFTER Data Quality so rules can evaluate plaintext!
    logger.info("Applying Column-Level Encryption to Valid Data")
    spii_cols = [c['name'] for c in validator.contract['columns'] if c.get('encrypt', False)]
    
    if spii_cols:
        kms_encryptor = KMSEnvelopeEncryption(args.kms_key_arn)
        kms_encryptor.generate_data_key()
        
        # Encrypt the valid DataFrame only!
        valid_df = kms_encryptor.encrypt_spii_columns(valid_df, spii_cols)
        
        # Save the ciphertext key to S3 for consumers
        logger.info(f"Saving encrypted data key to {args.keys_output_path}")
        save_ciphertext_key_to_s3(
            kms_encryptor.get_ciphertext_blob(),
            args.keys_output_path,
            args.execution_date,
            args.kms_key_arn
        )

    # 8. Write Valid Output
    logger.info(f"Writing valid data to {args.output_path}")
    
    # Add partition column for Hive-style partitioning
    from pyspark.sql.functions import lit
    valid_out = valid_df.withColumn("dt", lit(args.execution_date))

    valid_out.coalesce(4).write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .option("compression", "snappy") \
        .parquet(args.output_path)
        
    # 9. Write Rejected Output (if any)
    logger.info(f"Writing rejected data to {args.rejected_path}")
    rejected_out = rejected_df.withColumn("dt", lit(args.execution_date))
    
    rejected_out.coalesce(1).write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .json(args.rejected_path)

    logger.info("MongoDB to S3 ETL Job Completed Successfully.")

if __name__ == "__main__":
    main()
