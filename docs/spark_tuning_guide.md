# EMR Serverless Spark Tuning Guide

Unlike EMR on EC2 where you tune based on underlying instance sizes (e.g., `m5.xlarge`), **EMR Serverless** requires you to explicitly request exactly the vCPU and memory you need for the Driver and Executors.

## Our Configuration (`spark_tuning.json`)

For our 1 GB daily CSV workload, we configure the following limits via `sparkSubmitParameters` in the Job Run request:

```json
{
  "sparkSubmitParameters": [
    "--conf", "spark.executor.cores=4",
    "--conf", "spark.executor.memory=14g",
    "--conf", "spark.driver.cores=4",
    "--conf", "spark.driver.memory=14g",
    "--conf", "spark.dynamicAllocation.enabled=false",
    "--conf", "spark.sql.shuffle.partitions=8"
  ]
}
```

## The Rationale

### 1. The Worker Spec (`4 vCPU, 14 GB`)
EMR Serverless has predefined supported configurations. `4 vCPU` with `16 GB` of total memory is a standard tier.
* Why `14g` instead of `16g`? Spark requires `memoryOverhead` (default 10%). If we request 14g for the executor, Spark adds ~1.4g overhead, totalling 15.4g — which fits perfectly inside the 16 GB container limit without causing container OOM kills.

### 2. Disable Dynamic Allocation
`spark.dynamicAllocation.enabled=false`
* Why? For predictable, small workloads (1 GB), we don't want the overhead of Spark deciding to scale workers up and down mid-flight. We want to ask for exactly 2 workers, run the job fast, and stop.

### 3. Shuffle Partitions
`spark.sql.shuffle.partitions=8`
* Why? Spark defaults to `200` shuffle partitions. For a 1 GB file, a shuffle would result in 200 files of ~5MB each. This is incredibly inefficient and causes massive overhead. By setting it to 8, we get 8 files of ~125MB each, which perfectly balances across our 8 vCPUs (4 driver + 4 executor, though driver doesn't usually execute tasks, we actually have 2 executors later).
*(Note: We will pass `--conf spark.executor.instances=2` from Airflow dynamically).*

### 4. Adaptive Query Execution (AQE)
`spark.sql.adaptive.enabled=true`
* Why? AQE will automatically coalesce smaller partitions after a shuffle, preventing tiny file problems in S3.

## How to Test This Locally

If you install PySpark locally, you can test the script against the sample data:

```bash
# In the aws_data_engineer folder:
pip install pyspark

# Run the job locally
python spark_jobs/etl_main.py \
  --input_path data/sample/sample_input.csv \
  --output_path data/sample/output_parquet \
  --schema_path spark_jobs/config/schema_definition.json \
  --execution_date 2026-05-18
```
