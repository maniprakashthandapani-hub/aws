import os
import json
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

# -------------------------------------------------------------------
# Configuration & Variable Loading
# -------------------------------------------------------------------
def load_config():
    try:
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'pipeline_config.json')
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception:
        return {"aws_resources": {}}

local_config = load_config().get("aws_resources", {})

S3_BUCKET = Variable.get("S3_BUCKET", default_var=local_config.get("s3_bucket", "data-pipeline-dev-tmanipra"))
EMR_APP_ID = Variable.get("EMR_APP_ID", default_var=local_config.get("emr_serverless_app_id", "00g5ofepb7fr2k0t"))
JOB_ROLE_ARN = Variable.get("JOB_ROLE_ARN", default_var=local_config.get("emr_serverless_execution_role_arn", "arn:aws:iam::038849867257:role/EMR_Serverless_ExecutionRole"))
KMS_KEY_ARN = Variable.get("KMS_KEY_ARN", default_var=local_config.get("kms_key_arn", "arn:aws:kms:eu-west-2:038849867257:key/95c4b27f-2243-4e8d-a934-22c201b9e84d"))

# MongoDB connection string using Passwordless AWS IAM DB Authentication
# EMR role must be added to MongoDB Database Users under AWS IAM authentication
MONGO_URI = Variable.get(
    "MONGO_URI", 
    default_var="mongodb+srv://mongo.3emle8e.mongodb.net/data_lake_db?authSource=%24external&authMechanism=MONGODB-AWS"
)
MONGO_ROLE_ARN = Variable.get(
    "MONGO_ROLE_ARN",
    default_var=local_config.get("mongo_role_arn", "arn:aws:iam::038849867257:role/MongoDB_Atlas_AccessRole")
)

# -------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='daily_mongo_to_s3_etl',
    default_args=default_args,
    description='Daily PySpark MongoDB Atlas to S3 Data Lake ETL',
    schedule_interval='@daily',
    start_date=datetime(2026, 5, 17),
    catchup=False,
    tags=['emr-serverless', 'mongodb', 'etl', 'pyspark'],
) as dag:

    # Task: Submit MongoDB PySpark ETL Job to EMR Serverless
    # Since MongoDB is an operational datastore, we run this directly on a time schedule
    run_mongo_etl = EmrServerlessStartJobOperator(
        task_id='run_mongo_etl',
        application_id=EMR_APP_ID,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"s3://{S3_BUCKET}/scripts/etl_mongo_main.py",
                "entryPointArguments": [
                    "--mongo_uri", MONGO_URI,
                    "--mongo_role_arn", MONGO_ROLE_ARN,
                    "--output_path", f"s3://{S3_BUCKET}/processed/",
                    "--schema_path", f"s3://{S3_BUCKET}/config/schema_definition.json",
                    "--dq_rules_path", f"s3://{S3_BUCKET}/config/dq_rules.json",
                    "--rejected_path", f"s3://{S3_BUCKET}/rejected/",
                    "--kms_key_arn", KMS_KEY_ARN,
                    "--keys_output_path", f"s3://{S3_BUCKET}/config/keys/",
                    "--execution_date", "{{ ds }}"
                ],
                "sparkSubmitParameters": f"--py-files s3://{S3_BUCKET}/scripts/schema_validator.py,s3://{S3_BUCKET}/scripts/data_quality.py,s3://{S3_BUCKET}/scripts/encryption_utils.py --jars s3://{S3_BUCKET}/scripts/mongo-spark-connector_2.12-10.3.0.jar,s3://{S3_BUCKET}/scripts/mongodb-driver-sync-4.8.2.jar,s3://{S3_BUCKET}/scripts/mongodb-driver-core-4.8.2.jar,s3://{S3_BUCKET}/scripts/bson-4.8.2.jar,s3://{S3_BUCKET}/scripts/mongodb-crypt-1.5.2.jar --conf spark.executor.cores=4 --conf spark.executor.memory=14g --conf spark.driver.cores=4 --conf spark.driver.memory=14g --conf spark.dynamicAllocation.enabled=false --conf spark.sql.shuffle.partitions=8 --conf spark.sql.adaptive.enabled=true --conf spark.hadoop.fs.s3.enableServerSideEncryption=true --conf spark.hadoop.fs.s3.serverSideEncryption.kms.keyId={KMS_KEY_ARN}"
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{S3_BUCKET}/logs/emr-serverless/"
                }
            }
        },
        wait_for_completion=True,
        name="daily-mongo-etl-job-{{ ds }}"
    )
