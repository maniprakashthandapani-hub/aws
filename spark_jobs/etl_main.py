import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, coalesce, current_timestamp
from schema_validator import SchemaValidator
from data_quality import DataQualityEngine

def create_spark_session(app_name="DataPipelineETL"):
    """Initializes and returns a SparkSession."""
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

def encrypt_spii(df, columns_to_encrypt):
    """
    Placeholder for Phase 4 (Security & Encryption).
    Currently just passes the DataFrame through. 
    In Phase 4, we will integrate AWS KMS to AES-encrypt these columns.
    """
    print(f"Skipping encryption for {columns_to_encrypt} (to be implemented in Phase 4)")
    return df

def main():
    parser = argparse.ArgumentParser(description="PySpark ETL Job")
    parser.add_argument("--input_path", required=True, help="S3 path to input CSV")
    parser.add_argument("--output_path", required=True, help="S3 path to output Parquet")
    parser.add_argument("--schema_path", required=True, help="S3 or local path to schema.json")
    parser.add_argument("--dq_rules_path", required=True, help="S3 or local path to dq_rules.json")
    parser.add_argument("--rejected_path", required=True, help="S3 path to write rejected rows")
    parser.add_argument("--execution_date", required=True, help="Date partition (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = create_spark_session()
    logger = spark._jvm.org.apache.log4j.LogManager.getLogger("com.datapipeline.etl")
    logger.info(f"Starting ETL for execution date: {args.execution_date}")

    # 1. Load Contract & Get Schema
    validator = SchemaValidator(args.schema_path)
    spark_schema = validator.get_pyspark_schema()

    # 2. Read Data
    logger.info(f"Reading from {args.input_path}")
    df = spark.read.csv(
        args.input_path,
        header=True,
        schema=spark_schema,
        mode="PERMISSIVE" # Let schema_validator handle nulls/bad records
    )

    # 3. Validate & Standardise (Null strategies)
    df = validator.validate_and_standardise(df)

    # 4. Standardise Dates
    df = standardise_dates(df, "transaction_date")

    # 5. SPII Encryption (Phase 4 Hook)
    # Identify columns from contract that require encryption
    spii_cols = [c['name'] for c in validator.contract['columns'] if c.get('encrypt', False)]
    df = encrypt_spii(df, spii_cols)

    # 6. Metadata tracking
    df = df.withColumn("etl_processed_at", current_timestamp())

    # 7. Data Quality Checks (Phase 3)
    logger.info("Applying Data Quality Rules")
    dq_engine = DataQualityEngine(args.dq_rules_path)
    valid_df, rejected_df = dq_engine.apply_rules(df)

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
    # Note: We write this as JSON so it's easy for analysts to read the dq_failed_rules array
    logger.info(f"Writing rejected data to {args.rejected_path}")
    rejected_out = rejected_df.withColumn("dt", lit(args.execution_date))
    
    rejected_out.coalesce(1).write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .json(args.rejected_path)

    logger.info("ETL Job Completed Successfully.")

if __name__ == "__main__":
    main()
