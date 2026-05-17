# MVP Design — AWS EMR Serverless + PySpark + Airflow Data Pipeline

## 🎯 Goal

Build a **production-grade daily data pipeline** on AWS that reads CSV from S3, validates schema, applies transformations (null handling, date standardisation, SPII encryption), writes encrypted Parquet to S3, with full idempotency, archival, purging, logging — orchestrated by Airflow on a 7-day schedule, using **EMR Serverless** for zero-infrastructure compute.

## 📦 Repository

| Item | Value |
|------|-------|
| **GitHub** | `https://github.com/maniprakashthandapani-hub/aws.git` |
| **Local Path** | `c:\Users\91999\Downloads\Claude\aws_data_engineer` |
| **Branch** | `main` |
| **Status** | ✅ Git initialized, remote linked, Phase 1 initially committed |

---

## Architecture Overview

```
┌─────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  S3 Landing  │─────▶│ EMR Serverless  │─────▶│  S3 Processed   │
│  (CSV Input) │      │  (PySpark)      │      │  (Parquet/SSE)  │
└─────────────┘      └──────┬──────────┘      └────────┬────────┘
                            │                          │
                     ┌──────▼───────┐         ┌────────▼────────┐
                     │  S3 Logs     │         │  S3 Archive     │
                     │  (EMR/Spark) │         │  (2-day retain) │
                     └──────────────┘         └─────────────────┘
                            ▲                          │
                     ┌──────┴───────┐         ┌────────▼────────┐
                     │   Airflow    │         │  Glue Catalog   │
                     │   (MWAA/EC2) │         │  + Athena Query │
                     └──────────────┘         └─────────────────┘
                       Daily DAG (7 days)       Data Lake Layer
```

---

## User Review Required

> [!WARNING]  
> **Architectural Change: EMR Serverless**
> Switching from EMR on EC2 to EMR Serverless changes how we provision compute. We no longer manage EC2 instances or use Spot pricing directly. Instead, we pay per vCPU-hour and GB-hour.
> While Serverless is easier to manage, it removes the ability to use deep Spot discounts explicitly, making it slightly more expensive per run (~$0.23 vs ~$0.06) but still well within the $5 budget.
> **Please approve this architectural shift before we update the Phase 1 configuration files.**

---

## ⚡ Spark Tuning — 1 GB CSV Workload on EMR Serverless

### EMR Serverless Job Run Sizing

| Resource | Value |
|----------|-------|
| **Driver** | 4 vCPUs, 16 GB |
| **Executor** | 4 vCPUs, 16 GB |
| **Executor Count** | 2 |

### Recommended `spark-submit` Configuration

EMR Serverless accepts Spark configuration overrides directly in the Job Run request:

```json
{
  "sparkSubmitParameters": "--conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf spark.sql.shuffle.partitions=8 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.parquet.compression.codec=snappy --conf spark.hadoop.fs.s3a.server-side-encryption-algorithm=SSE-KMS --conf spark.hadoop.fs.s3a.server-side-encryption.key=<KMS_KEY_ARN> --conf spark.dynamicAllocation.enabled=false --conf spark.executor.cores=4 --conf spark.executor.memory=14g"
}
```

### Why These Values?

| Config | Value | Rationale |
|--------|-------|----------|
| `executor.memory` | `14g` | Comfortable headroom for 1GB CSV → transformations → Parquet write |
| `executor.cores` | `4` | Matches parallelism to data size; avoids thread contention |
| `num-executors` | `2` | 2 executors × 4 cores = 8 parallel tasks |
| `shuffle.partitions` | `8` | Default 200 is overkill for 1GB; 8 partitions ≈ 125MB each (ideal) |
| `AQE enabled` | `true` | Auto-coalesces small partitions, handles skew joins |
| `dynamicAllocation` | `false` | Fixed workers, no scaling overhead delays |

### File Output Sizing

```
1 GB CSV → ~250-350 MB Parquet (Snappy compression)
         → coalesce(4) = 4 files × ~75 MB each (ideal for Athena)
```

---

## S3 Bucket Layout

```
s3://my-data-pipeline-<account-id>/
├── landing/                    # Raw CSV files dropped here
│   └── YYYY/MM/DD/
├── processed/                  # Final Parquet output (SSE-KMS encrypted)
│   └── YYYY/MM/DD/
├── archive/                    # Processed input CSVs moved here
│   └── YYYY/MM/DD/            # Auto-purged after 2 days
├── rejected/                   # Failed quality check records
│   └── YYYY/MM/DD/
├── logs/
│   ├── emr-serverless/        # Serverless job logs
│   └── airflow/               # Airflow remote logs
├── scripts/                    # PySpark scripts & configs
│   ├── etl_main.py
│   ├── schema_validator.py
│   ├── data_quality.py
│   └── encryption_utils.py
└── config/
    └── schema_definition.json  # Expected schema contract
```

---

## 🏗️ Development Phases (Bird's Eye View)

### Phase 1 — AWS Infrastructure & IAM (Foundation)

| Item | Detail |
|------|--------|
| **EMR Serverless App** | EMR 7.x Spark application, auto-start/auto-stop enabled |
| **S3 Buckets** | Single bucket with prefix-based separation (see layout above) |
| **IAM Roles** | `EMR_Serverless_ExecutionRole`, custom policy for S3 + KMS |
| **KMS Key** | One CMK for S3 SSE-KMS encryption + column-level AES key |
| **VPC/Subnet** | Default VPC is fine for MVP; security group with minimal ingress |
| **Logging** | Job runs push logs to `s3://…/logs/emr-serverless/` |

**Key Deliverables:**
- [MODIFY] `infrastructure/iam_policies/emr_serverless_execution_role.json`
- [MODIFY] `infrastructure/emr_serverless_app_config.json`
- [MODIFY] `docs/phase1_infrastructure_iam.md` (Update manual steps)

---

### Phase 2 — PySpark Core ETL (`etl_main.py`)
*(No changes from previous plan — PySpark code runs identically)*

---

### Phase 3 — Data Quality Framework (`data_quality.py`)
*(No changes from previous plan)*

---

### Phase 4 — Security & Encryption
*(No changes from previous plan)*

---

### Phase 5 — Airflow Orchestration (DAG)

```python
# DAG: daily_emr_etl_pipeline
# Schedule: Daily for 7 days from start_date

start ──▶ check_source_file_exists
              │
              ▼
         start_emr_serverless_app
              │
              ▼
         submit_serverless_job_run (etl_main.py)
              │
              ▼
         monitor_job_run (sensor)
              │
              ▼
         validate_output_exists
              │
              ▼
         archive_source_file
              │
              ▼
         purge_old_archives (>2 days)
              │
              ▼
         stop_emr_serverless_app
              │
              ▼
           end
```

**Key Deliverables:**
- [MODIFY] `dags/daily_emr_etl.py` — Use `EmrServerlessStartJobRunOperator`
- [MODIFY] `docs/phase5_airflow_orchestration.md`

---

### Phase 6 — Operations, Monitoring & Maintenance
*(Metrics and alerts updated for EMR Serverless namespace instead of EMR on EC2)*

---

### Phase 7 — Data Lake & Athena Query Layer
*(No changes from previous plan)*

---

### Phase 8 — Infrastructure as Code (Terraform)
*(Terraform modules will map to `aws_emrserverless_application` instead of `aws_emr_cluster`)*

---

## 💰 Detailed Cost Analysis (Trial Account — 1 GB File, 7 Days)

> [!CAUTION]
> EMR Serverless charges by vCPU-hour and GB-hour. 

### Per-Run Cost Breakdown (1 GB CSV, ~10 min job)

| Resource | Config | Usage | Rate | Cost/Run |
|----------|--------|-------|------|----------|
| **Driver** | 4 vCPU, 16 GB | 0.16 hr (10m) | $0.05258/vCPU-hr, $0.00577/GB-hr | **$0.048** |
| **Executors** | 8 vCPU, 32 GB (2x) | 0.16 hr (10m) | $0.05258/vCPU-hr, $0.00577/GB-hr | **$0.096** |
| **S3 Storage** | 1.3 GB | Monthly | $0.023/GB | **$0.001** |
| **KMS/S3 APIs** | Requests | minimal | - | **$0.002** |
| | | | **Daily Total** | **~$0.147** |

### 7-Day Total Projection

| Component | 7-Day Cost |
|-----------|------------|
| EMR Serverless (compute) | $1.00 |
| S3 (storage + requests) | $0.02 |
| KMS (encryption calls) | $0.01 |
| Athena & Glue | $0.12 |
| **Grand Total (7 days)** | **~$1.15** |

> [!TIP]
> The total remains well under the $5 budget, and we no longer have to worry about cluster provisioning, EC2 Spot availability, or idle cluster costs.

---

## ✅ Definition of Done (MVP)

**ETL & Data Quality:**
- [ ] CSV read from S3 landing zone with explicit schema
- [ ] SPII columns encrypted with AES-256 (KMS key)
- [ ] Output written as Parquet with SSE-KMS encryption
- [ ] Data quality checks pass before write

**Spark Tuning:**
- [ ] Driver and Executors properly sized for Serverless limits
- [ ] `shuffle.partitions = 8`, AQE enabled
- [ ] `coalesce(4)` before write for optimal Athena file sizing

**Operations:**
- [ ] Source CSV archived after successful processing
- [ ] Archives older than 2 days auto-purged
- [ ] Airflow DAG runs daily for 7 days
- [ ] EMR Serverless application auto-stops after job
- [ ] Logs accessible in S3
- [ ] Failure alerts via SNS email

**Data Lake:**
- [ ] Parquet written with Hive-style partitioning (`dt=YYYY-MM-DD`)
- [ ] Athena can query processed data via SQL

**Infrastructure as Code (Terraform):**
- [ ] All manual resources codified in Terraform modules
- [ ] `terraform destroy` can tear down everything cleanly

**Cost:**
- [ ] Total 7-day cost stays under **$2.00**
- [ ] Budget alarm set at $5 safety threshold
