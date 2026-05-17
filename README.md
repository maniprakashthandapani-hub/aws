# AWS Data Pipeline — EMR Serverless + PySpark + Airflow

> Production-grade daily data pipeline on AWS that reads CSV from S3, validates schema, applies transformations (null handling, date standardisation, SPII encryption), writes encrypted Parquet to S3 — orchestrated by Airflow on a 7-day schedule.

## Architecture

```
S3 Landing (CSV) → EMR Serverless (PySpark) → S3 Processed (Parquet/SSE-KMS)
                         ↓                         ↓
                    S3 Logs              Glue Catalog + Athena
                         ↑
                    Airflow (Daily DAG, 7 days)
```

## Quick Start

1. **Read the phase docs** — `docs/phase1_infrastructure_iam.md` through `phase8_terraform_iac.md`
2. **Create AWS resources** — Follow console steps in Phase 1 doc
3. **Upload PySpark scripts** — `aws s3 sync spark_jobs/ s3://<BUCKET>/scripts/`
4. **Configure Airflow** — Deploy DAG from `airflow/dags/`
5. **Drop CSV to landing** — `aws s3 cp data.csv s3://<BUCKET>/landing/YYYY/MM/DD/`

## Project Structure

```
├── infrastructure/         # IAM policies, S3 config, KMS, EMR cluster config
├── spark_jobs/             # PySpark ETL code (etl_main.py, validators, encryption)
├── airflow/                # Airflow DAG and utility modules
├── athena/                 # SQL scripts for Glue/Athena setup and queries
├── terraform/              # Infrastructure as Code (Phase 8)
├── tests/                  # Unit tests for DQ, schema, encryption
├── data/sample/            # Sample test CSV (not committed)
└── docs/                   # Phase-by-phase explanation documents
```

## Key Features

| Feature | Implementation |
|---------|---------------|
| Schema Validation | Explicit schema contract (`schema_definition.json`) |
| Null Handling | Per-column strategy: drop / fill / flag |
| Date Standardisation | Cascading parse → `yyyy-MM-dd` |
| SPII Encryption | AES-256-GCM column-level + SSE-KMS file-level |
| Data Quality | 8 pre/post checks with configurable thresholds |
| Idempotency | Partition overwrite + date-based paths |
| Archival & Purge | S3 lifecycle: archive 2 days, rejected 30 days |
| Cost Controls | Spot instances, auto-terminate, budget alerts |
| Data Lake | Glue Catalog + Athena with Hive partitioning |

## Estimated Cost

| Period | Cost |
|--------|------|
| Per run (~8 min) | ~$0.06 |
| 7-day total | ~$0.57 |
| Safety budget | $5.00 |

## Documentation

See `docs/` for detailed phase-by-phase design documents with rationale, code samples, and AWS console setup guides.

## Repository

- **GitHub**: https://github.com/maniprakashthandapani-hub/aws.git
- **Region**: eu-west-2 (London)
