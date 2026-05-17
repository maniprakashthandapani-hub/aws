import os
import json
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.operators.python import PythonOperator

# -------------------------------------------------------------------
# Configuration & Variable Loading
# -------------------------------------------------------------------
# In a real production MWAA environment, these would be Airflow Variables.
# For this MVP, we try to load them from Variables, and fallback to the local pipeline_config.json
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

# -------------------------------------------------------------------
# Python Callables
# -------------------------------------------------------------------
def archive_landing_file(ds, **kwargs):
    """
    Moves the processed CSV from the landing zone to the archive zone.
    This ensures idempotency (so we don't re-process old files) and keeps the landing area clean.
    """
    s3 = boto3.client('s3')
    source_key = f"landing/{ds}/data.csv"
    dest_key = f"archive/{ds}/data.csv"
    
    print(f"Archiving s3://{S3_BUCKET}/{source_key} to s3://{S3_BUCKET}/{dest_key}")
    
    # 1. Copy to archive
    s3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={'Bucket': S3_BUCKET, 'Key': source_key},
        Key=dest_key,
        # SSE-KMS enforcement via bucket policy requires us to specify encryption when copying
        ServerSideEncryption='aws:kms',
        SSEKMSKeyId=KMS_KEY_ARN
    )
    
    # 2. Delete from landing
    s3.delete_object(Bucket=S3_BUCKET, Key=source_key)
    print("Archive complete.")

# -------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False, # In production, set to True and add email
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='daily_emr_serverless_etl',
    default_args=default_args,
    description='Daily PySpark ETL running on EMR Serverless',
    schedule_interval='@daily',
    start_date=datetime(2026, 5, 17), # Backfill will start from this date
    catchup=True, # Run historically for the past few days if needed
    tags=['emr-serverless', 'etl', 'pyspark'],
) as dag:

    # Task 1: Wait for the CSV file to arrive in the landing bucket for today's logical date
    # Uses {{ ds }} Jinja templating which evaluates to the execution date (YYYY-MM-DD)
    wait_for_input_file = S3KeySensor(
        task_id='wait_for_input_file',
        bucket_name=S3_BUCKET,
        bucket_key='landing/{{ ds }}/data.csv',
        wildcard_match=False,
        timeout=60 * 60 * 6, # Wait up to 6 hours
        poke_interval=60 * 5, # Check every 5 minutes
        mode='reschedule' # Don't block a worker slot while waiting
    )

    # Task 2: Submit the PySpark job to EMR Serverless
    # The application automatically wakes up because we configured Auto-Start in Phase 1!
    submit_spark_job = EmrServerlessStartJobOperator(
        task_id='run_pyspark_etl',
        application_id=EMR_APP_ID,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"s3://{S3_BUCKET}/scripts/etl_main.py",
                "entryPointArguments": [
                    "--input_path", f"s3://{S3_BUCKET}/landing/{{{{ ds }}}}/data.csv",
                    "--output_path", f"s3://{S3_BUCKET}/processed/{{{{ ds }}}}/",
                    "--schema_path", f"s3://{S3_BUCKET}/config/schema_definition.json",
                    "--dq_rules_path", f"s3://{S3_BUCKET}/config/dq_rules.json",
                    "--rejected_path", f"s3://{S3_BUCKET}/rejected/{{{{ ds }}}}/",
                    "--kms_key_arn", KMS_KEY_ARN,
                    "--keys_output_path", f"s3://{S3_BUCKET}/config/keys/",
                    "--execution_date", "{{ ds }}"
                ],
                "sparkSubmitParameters": f"--py-files s3://{S3_BUCKET}/scripts/schema_validator.py,s3://{S3_BUCKET}/scripts/dq_engine.py,s3://{S3_BUCKET}/scripts/encryption_utils.py --conf spark.executor.cores=4 --conf spark.executor.memory=14g --conf spark.driver.cores=4 --conf spark.driver.memory=14g --conf spark.dynamicAllocation.enabled=false --conf spark.sql.shuffle.partitions=8 --conf spark.sql.adaptive.enabled=true --conf spark.hadoop.fs.s3a.server-side-encryption-algorithm=SSE-KMS --conf spark.hadoop.fs.s3a.server-side-encryption.key={KMS_KEY_ARN}"
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{S3_BUCKET}/logs/emr-serverless/"
                }
            }
        },
        wait_for_completion=True, # Operator will block and wait for SUCCESS/FAILED status
        name="daily-etl-job-{{ ds }}"
    )

    # Task 3: Move the CSV file out of the landing zone into the archive zone
    archive_landing_file_task = PythonOperator(
        task_id='archive_landing_file',
        python_callable=archive_landing_file,
        provide_context=True # Passes standard Airflow context like `ds` into kwargs
    )

    # Define DAG Dependencies
    wait_for_input_file >> submit_spark_job >> archive_landing_file_task
