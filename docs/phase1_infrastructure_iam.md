# Phase 1 — AWS Infrastructure & IAM (Foundation)

## Why This Phase First?

Nothing runs without infrastructure. This phase creates the **security boundaries**, **storage layout**, **encryption keys**, and **serverless application blueprint** that every subsequent phase depends on. Think of it as laying the foundation of a building — PySpark (Phase 2), Airflow (Phase 5), and Athena (Phase 7) all stand on top of this.

---

## What We're Building

### 1. `.gitignore`

**Purpose:** Prevent secrets and junk from reaching GitHub.

| What's Excluded | Why |
|----------------|-----|
| `.env`, `*.pem`, `credentials` | AWS keys/secrets must never be in git |
| `__pycache__/`, `*.pyc` | Python bytecode — regenerated on every machine |
| `.idea/`, `.vscode/` | IDE-specific settings — personal to each developer |
| `*.log`, `*.tmp` | Transient files with no version control value |
| `data/sample/*.csv` | Sample data may be large; kept local only |

**Who cares about this?**
- **DevOps** — avoids credential leaks in CI/CD
- **App Maintenance** — clean repo, no noise in diffs

---

### 2. Project Folder Structure

```
aws_data_engineer/
├── infrastructure/          ← This phase
│   ├── emr_serverless_app_config.json
│   ├── iam_policies/
│   │   ├── emr_serverless_execution_role.json
│   │   ├── s3_kms_policy.json
│   │   └── athena_glue_policy.json
│   ├── s3_bucket_policy.json
│   ├── s3_lifecycle_rules.json
│   ├── kms_key_policy.json
│   └── glue_catalog_setup.json
├── spark_jobs/              ← Phase 2, 3, 4
├── airflow/                 ← Phase 5
├── athena/                  ← Phase 7
├── tests/                   ← Phase 2, 3
├── data/sample/             ← Phase 2
└── docs/                    ← All phases
```

**Why a single bucket with prefixes instead of multiple buckets?**
- Simpler IAM policies (one bucket ARN)
- Easier lifecycle management
- Lower cost (no cross-bucket transfer fees)
- Prefixes (`landing/`, `processed/`, `archive/`) act as logical separation

---

### 3. IAM Policies — The Security Layer

AWS uses **IAM Roles** to grant permissions. EMR Serverless requires an **Execution Role** to function:

#### 3a. EMR Serverless Execution Role (`emr_serverless_execution_role.json`)

**What it does:** This is the role that EMR Serverless assumes when running your Spark jobs. This is **what your PySpark code runs as**.

```
Your PySpark Code (running on EMR Serverless)
    ├── Reads CSV from s3://…/landing/       ← needs s3:GetObject
    ├── Writes Parquet to s3://…/processed/  ← needs s3:PutObject
    ├── Calls KMS to encrypt SPII columns    ← needs kms:Encrypt
    ├── Moves files to s3://…/archive/       ← needs s3:DeleteObject + PutObject
    └── Sends metrics to CloudWatch          ← needs cloudwatch:PutMetricData
```

**Without this:** Your Spark job would start but crash with `AccessDeniedException` the moment it tries to read from S3.

#### 3b. S3 + KMS Policy (`s3_kms_policy.json`)

**What it does:** Fine-grained S3 and KMS permissions attached to the Execution Role.

| Permission | Resource | Purpose |
|-----------|----------|---------|
| `s3:GetObject` | `landing/*`, `config/*`, `scripts/*` | Read input CSV, configs, PySpark scripts |
| `s3:PutObject` | `processed/*`, `archive/*`, `rejected/*`, `logs/*` | Write output, archive input, store rejects |
| `s3:DeleteObject` | `landing/*` | Remove source after archival (move = copy + delete) |
| `s3:ListBucket` | Bucket root | List files for existence checks |
| `kms:Encrypt` | Specific key ARN | Encrypt SPII columns + S3 SSE-KMS |
| `kms:Decrypt` | Specific key ARN | Decrypt if re-reading processed data |
| `kms:GenerateDataKey` | Specific key ARN | S3 SSE-KMS needs this for envelope encryption |

---

### 4. S3 Bucket Policy (`s3_bucket_policy.json`)

**What it does:** A bucket-level rule that **denies any upload** that doesn't use SSE-KMS encryption.

```
Any PUT to processed/ or archive/ ──▶ Check: Is SSE-KMS header present?
                                         ├── YES ──▶ Allow ✅
                                         └── NO  ──▶ Deny ❌ (403 Forbidden)
```

**Why not just rely on IAM?**
- IAM controls *who* can upload
- Bucket policy controls *how* they upload
- Defense-in-depth: even if someone has the right IAM role, they MUST encrypt

---

### 5. S3 Lifecycle Rules (`s3_lifecycle_rules.json`)

**What it does:** Automatically deletes objects in the `archive/` prefix after **2 days**.

```
Day 0: CSV processed → moved to archive/2026/05/17/data.csv
Day 1: Still in archive ✅
Day 2: Still in archive ✅
Day 3: AUTO-DELETED by S3 lifecycle ♻️
```

**Why use S3 lifecycle instead of code?**
- **Reliability** — S3 guarantees deletion; code can fail silently
- **Zero maintenance** — no cron job, no Airflow task, no Lambda
- **Auditable** — lifecycle rules are visible in AWS console
- **Cost** — no compute cost; S3 does it internally

---

### 6. KMS Key Policy (`kms_key_policy.json`)

**What it does:** Creates a Customer Managed Key (CMK) with specific access rules.

```
KMS Key: alias/data-pipeline-key
    ├── Admin: Root account (can manage key lifecycle)
    ├── Users: EMR Serverless Execution Role (can encrypt/decrypt)
    ├── Rotation: Enabled (AWS rotates key material annually)
    └── Grants: S3 service (for S3 SSE-KMS integration)
```

**Two encryption use cases for this single key:**
1. **Column-level** — PySpark `aes_encrypt()` uses a data key derived from this CMK
2. **File-level** — S3 SSE-KMS uses this CMK to encrypt the entire Parquet file

---

### 7. EMR Serverless Application Config (`emr_serverless_app_config.json`)

**What it does:** Complete EMR Serverless application specification — ready to be used by `aws emr-serverless create-application` CLI.

| Config Area | Setting | Why |
|------------|---------|-----|
| **Release** | `emr-7.x` | Latest Spark 3.5+ with AQE built-in |
| **Type** | `SPARK` | We only need Spark, no Hive/Presto |
| **Capacity** | Max 16 vCPU, 64 GB | Hard limit to prevent runaway costs |
| **Auto-start** | `true` | Starts automatically when job is submitted |
| **Auto-stop** | `15 min` idle | Stops billing automatically after job completes |

**Who cares about this?**
- **DevOps** — reproducible app via config file, no manual setup
- **App Maintenance** — zero infrastructure to manage (no EC2 instances)
- **Product Owner** — cost predictability with hard capacity limits

---

### 8. Athena/Glue IAM Policy (`athena_glue_policy.json`)

**What it does:** Allows a user or role to:
- Create/manage Glue databases and tables (catalog metadata)
- Run Athena queries against the processed data
- Write query results to S3

This is separate from the pipeline role because **data consumers** (analysts, dashboards) use Athena — they should NOT have EMR or landing zone access.

---

### 9. Glue Catalog Setup (`glue_catalog_setup.json`)

**What it does:** Defines the **data lake metadata**:
- Database name: `data_lake_db`
- Table name: `processed_data`
- Location: `s3://…/processed/`
- Partition key: `dt` (Hive-style `dt=YYYY-MM-DD`)

Athena reads the Glue table definition to know *where* the Parquet files are and *what schema* they have — then runs SQL directly against S3.

---

## 🖥️ Manual AWS Console Setup (Step-by-Step)

Do these in order — each step depends on the previous one.

> [!NOTE]
> All resources should be created in **eu-west-2 (London)**. Check the region selector in the top-right corner of the AWS console before each step.

### Console Step 1: Create S3 Bucket

```
AWS Console → S3 → Create bucket

┌──────────────────────────────────────────────────────────────┐
│  Create bucket                                                │
│                                                               │
│  Bucket name:     data-pipeline-dev-<YOUR-ACCOUNT-ID>         │
│                   (must be globally unique)                    │
│  Region:          EU (London) eu-west-2                       │
│                                                               │
│  Object Ownership:                                            │
│  ◉ ACLs disabled (recommended)                               │
│                                                               │
│  Block Public Access:                                         │
│  ☑ Block ALL public access  ← CRITICAL                      │
│                                                               │
│  Bucket Versioning:                                           │
│  ◉ Disable  (not needed for MVP — saves cost)                │
│                                                               │
│  Default encryption:                                          │
│  Encryption type:  ◉ Server-side encryption with AWS KMS     │
│  AWS KMS key:      ◉ AWS managed key (aws/s3)                │
│  (We'll change this to our CMK after creating it in Step 2)  │
│  ☑ Bucket Key: Enabled  (reduces KMS API costs)              │
│                                                               │
│                [ Create bucket ]                              │
└──────────────────────────────────────────────────────────────┘
```

**After creation — create folder prefixes:**
```
Click bucket name → Create folder → create each one:
  landing/
  processed/
  archive/
  rejected/
  logs/emr-serverless/
  logs/airflow/
  logs/dq_reports/
  scripts/
  config/
  athena-results/
```

---

### Console Step 2: Create KMS Key

```
AWS Console → KMS → Customer managed keys → Create key

┌──────────────────────────────────────────────────────────────┐
│  Step 1: Configure key                                        │
│  Key type:              ◉ Symmetric                          │
│  Key usage:             ◉ Encrypt and decrypt                │
│  Regionality:           ◉ Single-Region key                  │
│                                                               │
│  Step 2: Add labels                                           │
│  Alias:                 data-pipeline-key                     │
│  Description:           CMK for data pipeline encryption      │
│  Tags:                  Project = data-pipeline               │
│                         Environment = dev                     │
│                                                               │
│  Step 3: Define key administrative permissions                │
│  Key administrators:    ☑ Your IAM user                      │
│                                                               │
│  Step 4: Define key usage permissions                         │
│  Key users:             (leave empty for now — we'll add      │
│                          the Execution Role after creating it)│
│                                                               │
│                [ Finish ]                                     │
└──────────────────────────────────────────────────────────────┘
```

**After creation:**
1. Copy the **Key ARN** — you'll need it for IAM policies and Spark config
2. Note the **Key ID** (shorter UUID format)
3. Go back to S3 → bucket → Properties → Edit default encryption → change to your CMK

---

### Console Step 3: Create Execution Role

```
AWS Console → IAM → Roles → Create role

┌──────────────────────────────────────────────────────────────┐
│  Step 1: Select trusted entity                                │
│  Trusted entity type:  ◉ Custom trust policy                 │
│                                                               │
│  Custom trust policy:                                         │
│  {                                                            │
│    "Version": "2012-10-17",                                   │
│    "Statement": [                                             │
│      {                                                        │
│        "Effect": "Allow",                                     │
│        "Principal": { "Service": "emr-serverless.amazonaws.com" },│
│        "Action": "sts:AssumeRole"                             │
│      }                                                        │
│    ]                                                          │
│  }                                                            │
│                                                               │
│  Step 2: Add permissions                                      │
│  (Skip for now, we will add inline policy next)               │
│                                                               │
│  Step 3: Name, review, create                                │
│  Role name:            EMR_Serverless_ExecutionRole           │
│  Description:          Execution role for EMR Serverless jobs │
│                                                               │
│                [ Create role ]                                │
└──────────────────────────────────────────────────────────────┘
```

**After creation — add S3 and KMS permissions:**
```
IAM → Roles → EMR_Serverless_ExecutionRole → Add permissions → Create inline policy

JSON tab → copy the contents of infrastructure/iam_policies/s3_kms_policy.json
(Make sure to replace <BUCKET_NAME> and <KMS_KEY_ARN> with your values)

Policy name: data-pipeline-s3-kms-access
```

**After KMS policy — update the KMS key:**
```
KMS → Customer managed keys → data-pipeline-key → Key policy → Edit

Add EMR_Serverless_ExecutionRole to the "Key users" section so the
role can use the key for encryption/decryption.
```

---

### Console Step 4: Create EMR Serverless Application

```
AWS Console → EMR → EMR Serverless → Manage applications → Create application

┌──────────────────────────────────────────────────────────────┐
│  Name:                 data-pipeline-app                      │
│  Type:                 Spark                                  │
│  Release version:      emr-7.1.0                              │
│                                                               │
│  Architecture:         x86_64                                 │
│                                                               │
│  Application setup options:                                   │
│  ◉ Default setup (AWS configures VPC/subnets automatically)  │
│                                                               │
│  Application limits:                                          │
│  Maximum vCPU:         16 vCPU                                │
│  Maximum memory:       64 GB                                  │
│                                                               │
│  Application behavior:                                        │
│  ☑ Start application when job is submitted                  │
│  ☑ Stop application after being idle for: 15 minutes        │
│                                                               │
│                [ Create application ]                         │
└──────────────────────────────────────────────────────────────┘
```

**After creation:** Copy the **Application ID** (you will need this for Airflow).

---

### Console Step 5: Add S3 Lifecycle Rules

```
AWS Console → S3 → your bucket → Management → Create lifecycle rule

(Create 3 separate rules matching the s3_lifecycle_rules.json file)
1. Purge archives after 2 days (Prefix: archive/)
2. Purge Athena results after 7 days (Prefix: athena-results/)
3. Purge rejected records after 30 days (Prefix: rejected/)
```

---

### Console Step 6: Set Up Budget Alert

```
AWS Console → AWS Budgets → Create budget

(Set budget to $5.00 with 50%, 80%, 100% email alerts)
```

---

### Console Step 7: Verify Setup Checklist

Before moving to Phase 2, confirm everything exists:

```
□ S3 bucket created with 10 folder prefixes
□ S3 default encryption set to SSE-KMS with your CMK
□ S3 Block Public Access enabled
□ S3 lifecycle: 3 rules (archive/2d, athena-results/7d, rejected/30d)
□ KMS key created with alias "data-pipeline-key"
□ KMS key ARN noted down
□ IAM role: EMR_Serverless_ExecutionRole created
□ S3+KMS inline policy attached to role
□ Role added as KMS key user
□ EMR Serverless application created (App ID noted)
□ Budget alert set at $5
```

> [!IMPORTANT]
> **Save these values** — you'll need them in later phases:
> - S3 bucket name: `data-pipeline-dev-<ACCOUNT-ID>`
> - KMS Key ARN: `arn:aws:kms:eu-west-2:<ACCOUNT-ID>:key/<KEY-ID>`
> - Execution Role ARN: `arn:aws:iam::<ACCOUNT-ID>:role/EMR_Serverless_ExecutionRole`
> - EMR Serverless App ID: `<YOUR-APP-ID>`

---

## CLI Equivalent (Alternative to Console)

If you prefer CLI over the console, here are the equivalent commands:

```bash
# 1. Create S3 bucket
aws s3 mb s3://data-pipeline-dev-<ACCOUNT-ID> --region eu-west-2

# 2. Block public access
aws s3api put-public-access-block --bucket data-pipeline-dev-<ACCOUNT-ID> \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

# 3. Create KMS key
aws kms create-key --description "Data Pipeline Key" --region eu-west-2
aws kms create-alias --alias-name alias/data-pipeline-key --target-key-id <KEY-ID> --region eu-west-2
aws kms enable-key-rotation --key-id <KEY-ID> --region eu-west-2

# 4. Set bucket default encryption to CMK
aws s3api put-bucket-encryption --bucket data-pipeline-dev-<ACCOUNT-ID> \
  --server-side-encryption-configuration '{
    "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms", "KMSMasterKeyID": "<KEY-ARN>"}, "BucketKeyEnabled": true}]
  }'

# 5. Apply lifecycle rules
aws s3api put-lifecycle-configuration --bucket data-pipeline-dev-<ACCOUNT-ID> \
  --lifecycle-configuration file://infrastructure/s3_lifecycle_rules.json

# 6. Create IAM role
aws iam create-role --role-name EMR_Serverless_ExecutionRole \
  --assume-role-policy-document file://infrastructure/iam_policies/emr_serverless_execution_role.json
# (You still need to put the inline policy using put-role-policy)

# 7. Create EMR Serverless App
aws emr-serverless create-application \
  --cli-input-json file://infrastructure/emr_serverless_app_config.json \
  --region eu-west-2

# 8. Create budget
aws budgets create-budget --account-id <ACCOUNT-ID> \
  --budget file://infrastructure/budget_alert.json
```
