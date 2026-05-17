# Phase 5 — Airflow Orchestration

## Orchestrating EMR Serverless

One of the massive advantages of EMR Serverless over EMR on EC2 is how simple it makes our Airflow DAG. 

If we were using EMR on EC2, our DAG would look like this:
1. `CreateJobFlowOperator` (Wait 5-10 mins for EC2 instances to provision)
2. `EmrAddStepsOperator` (Submit PySpark code)
3. `EmrStepSensor` (Wait for code to finish)
4. `EmrTerminateJobFlowOperator` (Destroy EC2 instances to save money)

If Step 4 fails, the cluster stays running, costing you hundreds of dollars.

**With EMR Serverless, our DAG is just one task:**
1. `EmrServerlessStartJobRunOperator`

Because we configured **Auto-Start** and **Auto-Stop** in Phase 1, the EMR Application automatically spins up compute in seconds when Airflow submits the job, and automatically terminates it 15 minutes after the job finishes. No infrastructure management!

## The DAG Design (`daily_emr_etl.py`)

Our pipeline is scheduled to run `@daily`. Airflow passes in a logical execution date (`{{ ds }}`) to all our tasks, making the pipeline perfectly idempotent (rerunning yesterday's job will safely overwrite yesterday's partition, not today's).

### Task 1: `wait_for_input_file` (`S3KeySensor`)
- **What it does:** Wakes up every 5 minutes and checks if `s3://data-pipeline-.../landing/YYYY-MM-DD/data.csv` exists.
- **Why:** The PySpark job will fail instantly if there's no data to process. This sensor prevents the heavy EMR compute from spinning up until the data is actually ready.

### Task 2: `run_pyspark_etl` (`EmrServerlessStartJobRunOperator`)
- **What it does:** Uses the AWS API to tell EMR Serverless to run `etl_main.py`.
- **Config Injection:** We pass all our exact parameters (Input paths, Output paths, the KMS Key ARN) as `entryPointArguments`. We also override `sparkSubmitParameters` to enforce our 4 vCPU / 14 GB tuning.
- **Wait for Completion:** Airflow sits and monitors the job status until it returns `SUCCESS`.

### Task 3: `archive_landing_file` (`PythonOperator`)
- **What it does:** Uses `boto3` to copy the processed CSV from `landing/YYYY-MM-DD/` to `archive/YYYY-MM-DD/`, and then deletes the original.
- **Why:** 
  1. Cleans up the landing zone.
  2. Protects against accidental re-runs on old data.
  3. Prepares the file for our 2-day S3 Lifecycle Purge (created in Phase 1).

## Deployment

To deploy this in a real AWS environment:
1. **Upload Code:** Upload the `spark_jobs/` folder to `s3://.../scripts/` and `s3://.../config/`.
2. **Upload DAG:** Upload `airflow/dags/daily_emr_etl.py` to your MWAA (Managed Workflows for Apache Airflow) DAGs bucket.
3. **Set Connections:** In Airflow UI, ensure the `aws_default` connection has access to assume your EMR Execution Role.
