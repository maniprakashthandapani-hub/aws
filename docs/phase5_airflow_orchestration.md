# Phase 5 — Airflow Orchestration (DAG)

## Why This Phase Fifth?

Phases 2–4 built the PySpark code. But who **runs** it? When? What happens if it fails? What if S3 has no file today? Who terminates the cluster?

Airflow is the **orchestrator** — it doesn't process data, it manages the *workflow* of data processing. Think of it as a factory floor manager who coordinates machines (EMR), materials (S3 files), and quality inspectors (DQ checks) in the right order.

---

## DAG Overview

```
┌─────────────────────────────────────────────────────────────────┐
│              daily_emr_etl_pipeline (DAG)                        │
│              Schedule: @daily | Duration: 7 days                │
│                                                                  │
│  ┌──────────────────────┐                                       │
│  │ check_source_file    │  Does landing/ have a file today?     │
│  │ (S3KeySensor)        │  Wait up to 30 min, then fail.        │
│  └──────────┬───────────┘                                       │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────┐                                       │
│  │ create_emr_cluster   │  Spin up transient m5.xlarge cluster  │
│  │ (EmrCreateJobFlow)   │  Returns cluster_id via XCom          │
│  └──────────┬───────────┘                                       │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────┐                                       │
│  │ submit_spark_step    │  Submit etl_main.py as EMR Step       │
│  │ (EmrAddSteps)        │  with spark-submit config             │
│  └──────────┬───────────┘                                       │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────┐                                       │
│  │ watch_step           │  Poll EMR Step status every 60s       │
│  │ (EmrStepSensor)      │  DEFERRABLE — frees Airflow worker    │
│  └──────────┬───────────┘                                       │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────┐                                       │
│  │ validate_output      │  Check processed/ partition exists    │
│  │ (S3KeySensor)        │  Confirms Spark wrote successfully    │
│  └──────────┬───────────┘                                       │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────┐                                       │
│  │ repair_partitions    │  MSCK REPAIR TABLE in Athena          │
│  │ (AthenaOperator)     │  Makes new partition queryable        │
│  └──────────┬───────────┘                                       │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────┐                                       │
│  │ archive_source       │  Move CSV from landing/ → archive/    │
│  │ (PythonOperator)     │  Copy + Delete (S3 has no "move")     │
│  └──────────┬───────────┘                                       │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────┐                                       │
│  │ purge_old_archives   │  Delete archive files > 2 days old    │
│  │ (PythonOperator)     │  Belt-and-suspenders with lifecycle   │
│  └──────────┬───────────┘                                       │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────┐                                       │
│  │ terminate_cluster    │  Kill EMR cluster to stop billing     │
│  │ (EmrTerminateJobFlow)│  ALWAYS runs (trigger_rule=all_done) │
│  └──────────────────────┘                                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## DAG Configuration Explained

```python
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["data-team@company.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

dag = DAG(
    dag_id="daily_emr_etl_pipeline",
    default_args=default_args,
    description="Daily CSV → Parquet ETL on EMR with DQ checks",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 18),
    end_date=datetime(2026, 5, 25),      # 7 days only
    catchup=False,
    max_active_runs=1,
    tags=["emr", "etl", "production"],
)
```

### Why Each Config Matters

| Config | Value | Why |
|--------|-------|-----|
| `schedule_interval` | `@daily` | Runs once per day at midnight UTC |
| `start_date` | `2026-05-18` | First execution date |
| `end_date` | `2026-05-25` | Auto-stops after 7 runs — no manual intervention |
| `catchup` | `False` | Won't backfill missed days if DAG is paused |
| `max_active_runs` | `1` | Only one pipeline run at a time — prevents resource conflicts |
| `retries` | `2` | Auto-retry failed tasks twice before alerting |
| `retry_delay` | `5 min` | Wait between retries — gives transient AWS issues time to resolve |
| `execution_timeout` | `1 hour` | Kill the task if it hangs — prevents zombie clusters |
| `depends_on_past` | `False` | Today's run doesn't wait for yesterday's — independent runs |
| `email_on_failure` | `True` | Alert the team when retries exhaust |

---

## Task-by-Task Deep Dive

### Task 1: `check_source_file` (S3KeySensor)

```python
check_source = S3KeySensor(
    task_id="check_source_file",
    bucket_name=S3_BUCKET,
    bucket_key=f"landing/{{{{ ds_nodash[:4] }}}}/{{{{ ds_nodash[4:6] }}}}/{{{{ ds_nodash[6:8] }}}}/",
    wildcard_match=True,
    timeout=1800,           # Wait up to 30 minutes
    poke_interval=60,       # Check every 60 seconds
    mode="reschedule",      # Free worker slot between pokes
)
```

**Why a sensor instead of just starting the job?**
- Source files may arrive late (upstream delay)
- Without a sensor, the Spark job would start and immediately fail on empty input
- `mode="reschedule"` releases the worker slot — Airflow can run other tasks

**What if the file never arrives?**
- After 30 minutes, the sensor times out
- Task fails → retries 2 times (total 90 min window)
- After retries exhaust → email alert sent

**Who cares?**
- **App Maintenance** — clear failure mode: "file didn't arrive" vs "Spark crashed"
- **Product Owner** — 30-min grace window accommodates upstream delays

---

### Task 2: `create_emr_cluster` (EmrCreateJobFlowOperator)

```python
create_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=EMR_CLUSTER_CONFIG,  # From emr_config.py
    aws_conn_id="aws_default",
)
```

**Key behaviour:**
- Creates a **transient** cluster (not long-running)
- Returns `cluster_id` via **XCom** (Airflow's inter-task communication)
- Subsequent tasks reference this `cluster_id` to add steps and terminate

**Why transient, not a shared persistent cluster?**
| Aspect | Transient | Persistent |
|--------|-----------|------------|
| Cost | Pay only during job (~8 min/day) | Pay 24/7 even when idle |
| Isolation | Fresh cluster every run | Shared resources, potential contention |
| Failure blast radius | One day fails, others unaffected | Cluster crash affects all jobs |
| Config changes | Each run can use different config | Requires cluster restart |
| **For 1 job/day on trial account** | ✅ **Correct choice** | ❌ Overkill |

---

### Task 3: `submit_spark_step` (EmrAddStepsOperator)

```python
submit_step = EmrAddStepsOperator(
    task_id="submit_spark_step",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    steps=[{
        "Name": "ETL_Main_{{ ds }}",
        "ActionOnFailure": "CONTINUE",  # Don't terminate cluster on step failure
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--driver-memory", "2g",
                "--executor-memory", "4g",
                "--executor-cores", "2",
                "--num-executors", "2",
                "--conf", "spark.sql.adaptive.enabled=true",
                "--conf", "spark.sql.shuffle.partitions=8",
                "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                "--conf", "spark.sql.parquet.compression.codec=snappy",
                "s3://my-data-pipeline/scripts/etl_main.py",
                "--execution-date", "{{ ds }}",
                "--input-path", "s3://my-data-pipeline/landing/{{ ds_nodash[:4] }}/{{ ds_nodash[4:6] }}/{{ ds_nodash[6:8] }}/",
                "--output-path", "s3://my-data-pipeline/processed/",
            ]
        }
    }],
)
```

**Key details:**
- `ActionOnFailure=CONTINUE` — if the step fails, the cluster stays alive so we can read logs before terminating
- Execution date passed as argument — PySpark uses this for partition naming (idempotency)
- All Spark tuning from Phase 2's Spark Tuning section is embedded here

**Why `--deploy-mode cluster`?**
- Driver runs on the **master node**, not on the Airflow worker
- If Airflow loses connection, the Spark job continues running
- Logs are on EMR, not on the Airflow machine

---

### Task 4: `watch_step` (EmrStepSensor — Deferrable)

```python
watch_step = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_step', key='return_value')[0] }}",
    deferrable=True,        # KEY: frees the worker slot
    poke_interval=60,
)
```

**Why `deferrable=True`?**
```
Without deferrable:                With deferrable:
Worker slot BLOCKED for 8 min     Worker slot FREE during 8 min
┌──────────────────┐              ┌──────────────────┐
│ Worker 1: BUSY   │              │ Worker 1: FREE   │
│ (just polling)   │              │ (runs other DAGs)│
└──────────────────┘              └──────────────────┘
```

In a small Airflow setup (e.g., MWAA starter), you may only have 2 workers. Blocking one for 8 minutes of polling is wasteful.

---

### Task 5: `validate_output` (S3KeySensor)

```python
validate_output = S3KeySensor(
    task_id="validate_output",
    bucket_name=S3_BUCKET,
    bucket_key=f"processed/dt={{{{ ds }}}}/",
    wildcard_match=True,
    timeout=120,
    poke_interval=30,
)
```

**Why validate output separately?**
- Spark job may report "SUCCESS" but write to the wrong path
- A bug in partition naming could write to `dt=2026-05-81` (impossible date)
- This sensor confirms the **expected** partition actually exists

---

### Task 6: `repair_partitions` (AthenaOperator)

```python
repair_partitions = AthenaOperator(
    task_id="repair_partitions",
    query="MSCK REPAIR TABLE data_lake_db.processed_data",
    database="data_lake_db",
    output_location=f"s3://{S3_BUCKET}/athena-results/",
    aws_conn_id="aws_default",
)
```

**What does `MSCK REPAIR TABLE` do?**
- Scans S3 for new partitions (e.g., `dt=2026-05-18/`)
- Registers them in the Glue Catalog
- Makes them immediately queryable in Athena
- No Glue Crawler needed — cheaper and faster

---

### Task 7: `archive_source` (PythonOperator)

```python
def archive_source_file(**context):
    """Move source CSV from landing/ to archive/."""
    s3 = boto3.client("s3")
    ds = context["ds"]
    
    source_prefix = f"landing/{ds[:4]}/{ds[5:7]}/{ds[8:10]}/"
    archive_prefix = f"archive/{ds[:4]}/{ds[5:7]}/{ds[8:10]}/"
    
    # S3 has no "move" — it's copy + delete
    objects = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=source_prefix)
    
    for obj in objects.get("Contents", []):
        # Copy to archive
        s3.copy_object(
            Bucket=S3_BUCKET,
            CopySource={"Bucket": S3_BUCKET, "Key": obj["Key"]},
            Key=obj["Key"].replace("landing/", "archive/"),
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=KMS_KEY_ARN,
        )
        # Delete from landing
        s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
```

**Why copy + delete instead of a single "move"?**
- S3 doesn't have a move operation — it's always copy + delete
- We encrypt the archive copy with SSE-KMS (the source CSV might not be encrypted)
- If copy succeeds but delete fails → data exists in both places (safe, idempotent)
- If delete succeeds but copy fails → impossible (we copy first)

---

### Task 8: `purge_old_archives` (PythonOperator)

```python
def purge_old_archives(**context):
    """Delete archive files older than 2 days (belt-and-suspenders with lifecycle)."""
    s3 = boto3.client("s3")
    cutoff = datetime.now() - timedelta(days=2)
    
    objects = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="archive/")
    for obj in objects.get("Contents", []):
        if obj["LastModified"].replace(tzinfo=None) < cutoff:
            s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
            logger.info(f"Purged: {obj['Key']}")
```

**Why code purge + S3 lifecycle?**
- S3 lifecycle runs once per day (not real-time) — exact timing unpredictable
- Code purge runs at a known time (part of DAG) — predictable
- If one mechanism fails, the other catches it
- Belt and suspenders — critical for compliance audits

---

### Task 9: `terminate_cluster` (EmrTerminateJobFlowOperator)

```python
terminate_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    trigger_rule="all_done",    # CRITICAL: runs even if upstream fails
)
```

**Why `trigger_rule="all_done"` is CRITICAL:**

```
Default trigger_rule="all_success":     trigger_rule="all_done":
─────────────────────────────           ─────────────────────────
Spark step FAILS                        Spark step FAILS
    │                                       │
    ▼                                       ▼
terminate_cluster SKIPPED ❌             terminate_cluster RUNS ✅
    │                                       │
    ▼                                       ▼
Cluster keeps running 💸💸💸             Cluster terminated 💰
(You pay until auto-terminate            (Immediate cost savings)
 at 15 min idle timeout)
```

**Who cares?**
- **DevOps** — no surprise bills from orphaned clusters
- **App Maintenance** — cluster cleanup is guaranteed, even on failures
- **Product Owner** — cost guardrail enforced at the orchestration level

---

## Idempotency Deep Dive

**Idempotent** means: running the same DAG for the same date twice produces the **exact same result** with no duplicates.

### How Each Task is Idempotent

| Task | Idempotency Mechanism |
|------|----------------------|
| `check_source_file` | Sensor — re-checking is harmless |
| `create_emr_cluster` | Creates a new cluster (old one terminated) |
| `submit_spark_step` | PySpark writes with `mode("overwrite")` per partition |
| `validate_output` | Sensor — re-checking is harmless |
| `repair_partitions` | `MSCK REPAIR TABLE` is idempotent by design |
| `archive_source` | Copy is idempotent; delete is idempotent |
| `purge_old_archives` | Deleting already-deleted files is a no-op |
| `terminate_cluster` | Terminating already-terminated cluster is a no-op |

### Backfill Safety

```bash
# Airflow CLI — reprocess May 18th
airflow dags backfill daily_emr_etl_pipeline \
    --start-date 2026-05-18 \
    --end-date 2026-05-18
```

This will:
1. Create a fresh EMR cluster
2. Re-read the CSV from `landing/` (if still there) or `archive/` (manual restore)
3. Overwrite `processed/dt=2026-05-18/` — no duplicates
4. Update Glue catalog

---

## Airflow Modules (Helper Files)

### `utils/emr_config.py`

Centralises EMR cluster configuration:
```python
def get_emr_config(execution_date):
    """Returns cluster config dict for EmrCreateJobFlowOperator."""
    return {
        "Name": f"etl-cluster-{execution_date}",
        "ReleaseLabel": "emr-7.1.0",
        "Applications": [{"Name": "Spark"}],
        "Instances": { ... },        # m5.xlarge, Spot, etc.
        "Steps": [],                  # Steps added separately
        "AutoTerminationPolicy": {"IdleTimeout": 900},
        "LogUri": f"s3://{S3_BUCKET}/logs/emr/",
        ...
    }
```

### `utils/s3_helpers.py`

Reusable S3 operations:
```python
def file_exists(bucket, prefix) -> bool
def move_files(bucket, source_prefix, dest_prefix, kms_key)
def purge_older_than(bucket, prefix, days) -> int
def get_file_list(bucket, prefix) -> list
```

---

## Airflow Logging Best Practices

### Remote Logging to S3

```python
# airflow.cfg or environment variable
AIRFLOW__LOGGING__REMOTE_LOGGING = True
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER = s3://my-data-pipeline/logs/airflow/
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID = aws_default
```

**Why remote logging?**
- If the Airflow worker crashes, logs survive in S3
- Searchable across all DAG runs
- Required for MWAA (managed Airflow) — no local disk

### What to Log in Custom Tasks

```python
import logging
logger = logging.getLogger(__name__)

def archive_source_file(**context):
    ds = context["ds"]
    logger.info(f"[ARCHIVE] Starting archive for date={ds}")
    logger.info(f"[ARCHIVE] Source: landing/{ds}/")
    
    # ... archive logic ...
    
    logger.info(f"[ARCHIVE] Moved {count} files to archive/{ds}/")
    logger.info(f"[ARCHIVE] Deleted {count} files from landing/{ds}/")
```

**Logging conventions:**
- Prefix with `[TASK_NAME]` for easy grep in S3 logs
- Log inputs (what date, what path) and outputs (how many files, row counts)
- Log timing for performance tracking

---

## Failure Handling & Alerts

### SNS Email Alert on Failure

```python
from airflow.providers.amazon.aws.hooks.sns import SnsHook

def failure_callback(context):
    """Called when a task fails after all retries."""
    sns = SnsHook(aws_conn_id="aws_default")
    sns.publish_to_target(
        target_arn=SNS_TOPIC_ARN,
        message=f"Pipeline FAILED\n"
                f"DAG: {context['dag'].dag_id}\n"
                f"Task: {context['task'].task_id}\n"
                f"Date: {context['ds']}\n"
                f"Log: {context['task_instance'].log_url}",
        subject="⚠️ ETL Pipeline Failure Alert",
    )
```

---

## Files Produced in This Phase

| File | Purpose |
|------|---------|
| `airflow/dags/daily_emr_etl.py` | Production DAG with 9 tasks |
| `airflow/utils/emr_config.py` | Cluster config helper |
| `airflow/utils/s3_helpers.py` | S3 utility functions |

---

## Airflow Connection Setup

Before the DAG can run, configure the AWS connection in Airflow:

```bash
airflow connections add aws_default \
    --conn-type aws \
    --conn-extra '{"region_name": "eu-west-2"}'
```

For MWAA (managed Airflow), the connection uses the execution role automatically — no credentials needed.

> [!IMPORTANT]
> Never store AWS access keys in Airflow connections in production. Use **IAM roles** (EC2 instance profile or MWAA execution role) for credential-free authentication.
