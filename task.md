# Task Tracker — AWS EMR Data Pipeline MVP

## Phase Progress

- [x] **Phase 1** — AWS Infrastructure & IAM (Foundation)
- [x] **Phase 2** — PySpark Core ETL
- [x] **Phase 3** — Data Quality Framework
- [x] **Phase 4** — Security & Encryption
- [/] **Phase 5** — Airflow Orchestration
- [ ] **Phase 6** — Operations, Monitoring & Maintenance
- [ ] **Phase 7** — Data Lake & Athena Query Layer
- [ ] **Phase 8** — Terraform IaC

---

## Phase 1 — AWS Infrastructure & IAM ✅
- [x] `.gitignore`
- [x] Project folder scaffold (7 directories)
- [x] IAM policy: EMR Serverless Execution Role (`emr_serverless_execution_role.json`)
- [x] IAM policy: S3 + KMS access (`s3_kms_policy.json`)
- [x] IAM policy: Athena + Glue (`athena_glue_policy.json`)
- [x] S3 bucket policy — enforce encryption (`s3_bucket_policy.json`)
- [x] S3 lifecycle rules — archive 2-day purge (`s3_lifecycle_rules.json`)
- [x] KMS key policy (`kms_key_policy.json`)
- [x] EMR Serverless App config JSON (`emr_serverless_app_config.json`)
- [x] Glue catalog setup (`glue_catalog_setup.json`)
- [x] README.md
- [x] Git commit & push Phase 1

## Phase 2 — PySpark Core ETL ✅
- [x] `spark_jobs/etl_main.py`
- [x] `spark_jobs/schema_validator.py`
- [x] `spark_jobs/config/schema_definition.json`
- [x] `spark_jobs/config/spark_tuning.json`
- [x] `data/sample/sample_input.csv`
- [x] `docs/spark_tuning_guide.md`
- [x] Git commit & push Phase 2
## Phase 3 — Data Quality Framework ✅
- [x] `spark_jobs/data_quality.py`
- [x] `spark_jobs/config/dq_rules.json`
- [x] Update `spark_jobs/etl_main.py` for DQ routing
- [x] `docs/phase3_data_quality.md`
- [x] Git commit & push Phase 3
## Phase 4 — Security & Encryption ✅
- [x] `spark_jobs/encryption_utils.py`
- [x] Update `spark_jobs/etl_main.py` to use KMS Envelope Encryption
- [x] `docs/phase4_security_encryption.md`
- [x] Git commit & push Phase 4
