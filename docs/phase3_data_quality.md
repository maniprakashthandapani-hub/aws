# Phase 3 — Data Quality Framework

## The Problem
Data pipelines inevitably encounter bad data. If we write bad data into our Data Lake (S3 Processed zone), it breaks downstream analytics and machine learning models. 

## The Solution
We implemented a **Row-Level Data Quality Engine** (`data_quality.py`). Instead of hardcoding `if/else` statements in PySpark, we defined a JSON ruleset (`config/dq_rules.json`) that the engine reads dynamically.

### How It Works: The Split-Stream Pattern
When the pipeline runs, the Data Quality Engine evaluates every row against the rules.
1. If a row passes all rules, it goes into the **Valid Stream**.
2. If a row fails *any* rule, it goes into the **Rejected Stream**, and an array of the failed rule IDs (`dq_failed_rules`) is attached to that row.

```
Incoming DataFrame ──▶ DataQualityEngine ──┬──▶ Valid DataFrame    ──▶ Parquet (s3://.../processed/)
                                           │
                                           └──▶ Rejected DataFrame ──▶ JSON (s3://.../rejected/)
```

## Why write Rejected Rows to S3?
**Data Observability!** By saving the rejected rows as JSON with the `dq_failed_rules` array, Data Engineers can easily query the `rejected/` bucket to see exactly *which* rows failed and *why*.
- **Why JSON instead of Parquet for rejects?** JSON natively supports arrays (like `dq_failed_rules`) without requiring complex schema definitions in Athena, making it easier to debug quickly.
- Remember the **S3 Lifecycle Rule** we created in Phase 1? The `rejected/` folder will automatically purge these files after 30 days to save cost!

## The Rules Engine (`dq_rules.json`)
Currently supports 3 rule types:
- `not_null`: Fails if the column is null.
- `range`: Fails if the column is outside the `min`/`max` bounds.
- `regex`: Fails if the column value does not match the regular expression pattern.

This means Product Owners or Analysts can add new Data Quality rules by just updating the JSON file — no PySpark code changes required!

## Running the Job locally (Updated Command)
```bash
python spark_jobs/etl_main.py \
  --input_path data/sample/sample_input.csv \
  --output_path data/sample/output_parquet \
  --schema_path spark_jobs/config/schema_definition.json \
  --dq_rules_path spark_jobs/config/dq_rules.json \
  --rejected_path data/sample/output_rejected \
  --execution_date 2026-05-18
```
