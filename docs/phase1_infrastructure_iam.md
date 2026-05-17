# Phase 1 — AWS Infrastructure & IAM (Foundation)

## Why This Phase First?

Nothing runs without infrastructure. This phase creates the **security boundaries**, **storage layout**, **encryption keys**, and **cluster blueprint** that every subsequent phase depends on. Think of it as laying the foundation of a building — PySpark (Phase 2), Airflow (Phase 5), and Athena (Phase 7) all stand on top of this.

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
│   ├── emr_cluster_config.json
│   ├── iam_policies/
│   │   ├── emr_service_role.json
│   │   ├── emr_ec2_role.json
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

AWS uses **IAM Roles** to grant permissions. EMR requires **two roles** to function:

#### 3a. EMR Service Role (`emr_service_role.json`)

**What it does:** Allows the EMR *service itself* to manage AWS resources on your behalf.

```
EMR Service ──▶ Creates EC2 instances for your cluster
             ──▶ Attaches EBS volumes for local storage
             ──▶ Manages security groups for networking
             ──▶ Writes cluster logs to S3
```

**Without this:** EMR cannot create a single instance. The "Create Cluster" button would fail immediately.

**Permissions granted:**
- `ec2:RunInstances`, `ec2:TerminateInstances` — launch/kill cluster nodes
- `ec2:CreateSecurityGroup` — networking for cluster communication
- `s3:PutObject` on logs prefix — write EMR step/application logs

#### 3b. EMR EC2 Instance Role (`emr_ec2_role.json`)

**What it does:** Attached to every EC2 instance *inside* the cluster. This is **what your PySpark code runs as**.

```
Your PySpark Code (running on EC2)
    ├── Reads CSV from s3://…/landing/       ← needs s3:GetObject
    ├── Writes Parquet to s3://…/processed/  ← needs s3:PutObject
    ├── Calls KMS to encrypt SPII columns    ← needs kms:Encrypt
    ├── Moves files to s3://…/archive/       ← needs s3:DeleteObject + PutObject
    └── Sends metrics to CloudWatch          ← needs cloudwatch:PutMetricData
```

**Without this:** Your Spark job would start but crash with `AccessDeniedException` the moment it tries to read from S3.

**Key principle — Least Privilege:**
- Can read ONLY from `landing/` and `config/` prefixes
- Can write ONLY to `processed/`, `archive/`, `rejected/`, and `logs/`
- Can use ONLY the specific KMS key we create (not all keys in the account)

#### 3c. S3 + KMS Policy (`s3_kms_policy.json`)

**What it does:** Fine-grained S3 and KMS permissions attached to the EC2 role.

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

**Who cares about this?**
- **DevOps** — enforces compliance without trusting application code
- **Product Owner** — assurance that data-at-rest is always encrypted
- **Data Consumer** — confidence that their PII is protected

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

**Who cares about this?**
- **App Maintenance** — no custom purge script to debug
- **DevOps** — infrastructure-as-code, not application logic
- **Product Owner** — data retention policy enforced automatically

---

### 6. KMS Key Policy (`kms_key_policy.json`)

**What it does:** Creates a Customer Managed Key (CMK) with specific access rules.

```
KMS Key: alias/data-pipeline-key
    ├── Admin: Root account (can manage key lifecycle)
    ├── Users: EMR EC2 role (can encrypt/decrypt)
    ├── Rotation: Enabled (AWS rotates key material annually)
    └── Grants: EMR service (for S3 SSE-KMS integration)
```

**Two encryption use cases for this single key:**
1. **Column-level** — PySpark `aes_encrypt()` uses a data key derived from this CMK
2. **File-level** — S3 SSE-KMS uses this CMK to encrypt the entire Parquet file

**Key rotation** means AWS creates new key material yearly, but old data encrypted with the old material can still be decrypted. Zero downtime, zero code changes.

---

### 7. EMR Cluster Config (`emr_cluster_config.json`)

**What it does:** Complete cluster specification — ready to be used by `aws emr create-cluster` CLI or Airflow's `EmrCreateJobFlowOperator`.

| Config Area | Setting | Why |
|------------|---------|-----|
| **Release** | `emr-7.x` | Latest Spark 3.5+ with AQE built-in |
| **Master** | `m5.xlarge`, On-Demand | Stability — driver runs here, single point of failure |
| **Core** | `m5.xlarge`, **Spot** | Cost savings (60% off); executors run here |
| **Instances** | 1 master + 1 core | Minimum viable for 1GB workload |
| **Auto-terminate** | 15 min idle | Safety net — kills cluster if Airflow fails to terminate |
| **Logging** | `s3://…/logs/emr/` | Persistent logs survive cluster termination |
| **Spark configs** | driver=2g, executor=4g, shuffle=8 | Tuned for 1GB (see Spark Tuning section) |
| **Applications** | Spark only | No Hive/Pig/HBase — keep it lean |

**Who cares about this?**
- **DevOps** — reproducible cluster via config file, not console clicking
- **App Maintenance** — change instance type or Spark config in one place
- **Product Owner** — cost predictable (~$0.06/run)

---

### 8. Athena/Glue IAM Policy (`athena_glue_policy.json`)

**What it does:** Allows a user or role to:
- Create/manage Glue databases and tables (catalog metadata)
- Run Athena queries against the processed data
- Write query results to S3

This is separate from the EMR role because **data consumers** (analysts, dashboards) use Athena — they should NOT have EMR or landing zone access.

---

### 9. Glue Catalog Setup (`glue_catalog_setup.json`)

**What it does:** Defines the **data lake metadata**:
- Database name: `data_lake_db`
- Table name: `processed_data`
- Location: `s3://…/processed/`
- Partition key: `dt` (Hive-style `dt=YYYY-MM-DD`)
- Schema: Auto-discovered from Parquet or manually defined

**Think of Glue Catalog as:**
```
Traditional Database          vs.          Data Lake
─────────────────                          ─────────
PostgreSQL schema                          Glue Catalog
  └── table definition                       └── table definition
      └── data on disk                           └── data in S3 (Parquet)
```

Athena reads the Glue table definition to know *where* the Parquet files are and *what schema* they have — then runs SQL directly against S3.

---

## Files Produced in This Phase

| File | Purpose |
|------|---------|
| `.gitignore` | Exclude secrets, bytecode, IDE files |
| `infrastructure/iam_policies/emr_service_role.json` | EMR service permissions |
| `infrastructure/iam_policies/emr_ec2_role.json` | EC2 instance profile for Spark |
| `infrastructure/iam_policies/s3_kms_policy.json` | S3 read/write + KMS encrypt/decrypt |
| `infrastructure/iam_policies/athena_glue_policy.json` | Athena query + Glue catalog access |
| `infrastructure/s3_bucket_policy.json` | Enforce SSE-KMS on uploads |
| `infrastructure/s3_lifecycle_rules.json` | Auto-purge archive after 2 days |
| `infrastructure/kms_key_policy.json` | CMK access control + rotation |
| `infrastructure/emr_cluster_config.json` | Full cluster specification |
| `infrastructure/glue_catalog_setup.json` | Glue database + table definition |
| `README.md` | Project overview |

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
  logs/emr/
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
│                          the EMR EC2 role after creating it)  │
│                                                               │
│                [ Finish ]                                     │
└──────────────────────────────────────────────────────────────┘
```

**After creation:**
1. Copy the **Key ARN** — you'll need it for IAM policies and Spark config
2. Note the **Key ID** (shorter UUID format)
3. Go back to S3 → bucket → Properties → Edit default encryption → change to your CMK

---

### Console Step 3: Create IAM Roles

#### 3a. EMR Service Role

```
AWS Console → IAM → Roles → Create role

┌──────────────────────────────────────────────────────────────┐
│  Step 1: Select trusted entity                                │
│  Trusted entity type:  ◉ AWS service                         │
│  Use case:             EMR                                    │
│  Select:               ◉ EMR                                 │
│                                                               │
│  Step 2: Add permissions                                      │
│  Search and select:                                           │
│  ☑ AmazonEMRServicePolicy_v2                                │
│                                                               │
│  Step 3: Name, review, create                                │
│  Role name:            EMR_DefaultRole                        │
│  Description:          Allows EMR to manage cluster resources │
│  Tags:                 Project = data-pipeline                │
│                                                               │
│                [ Create role ]                                │
└──────────────────────────────────────────────────────────────┘
```

#### 3b. EMR EC2 Instance Profile Role

```
AWS Console → IAM → Roles → Create role

┌──────────────────────────────────────────────────────────────┐
│  Step 1: Select trusted entity                                │
│  Trusted entity type:  ◉ AWS service                         │
│  Use case:             EC2                                    │
│  Select:               ◉ EC2                                 │
│                                                               │
│  Step 2: Add permissions                                      │
│  Search and select:                                           │
│  ☑ AmazonS3FullAccess      (we'll restrict this later)      │
│  ☑ AmazonEMRForEC2Role     (deprecated but works for MVP)   │
│                                                               │
│  Step 3: Name, review, create                                │
│  Role name:            EMR_EC2_DefaultRole                    │
│  Description:          EC2 instance profile for EMR nodes     │
│                                                               │
│                [ Create role ]                                │
└──────────────────────────────────────────────────────────────┘
```

**After creation — add KMS permissions:**
```
IAM → Roles → EMR_EC2_DefaultRole → Add permissions → Create inline policy

JSON tab → paste:
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:GenerateDataKey",
        "kms:DescribeKey"
      ],
      "Resource": "<YOUR-KMS-KEY-ARN>"
    }
  ]
}

Policy name: data-pipeline-kms-access
```

**After KMS policy — update the KMS key:**
```
KMS → Customer managed keys → data-pipeline-key → Key policy → Edit

Add EMR_EC2_DefaultRole to the "Key users" section so the
role can use the key for encryption/decryption.
```

---

### Console Step 4: Add S3 Lifecycle Rules

```
AWS Console → S3 → your bucket → Management → Create lifecycle rule

┌──────────────────────────────────────────────────────────────┐
│  Rule 1: Purge archives                                       │
│  Rule name:           purge-archive-after-2-days              │
│  Status:              ◉ Enabled                              │
│  Filter:              ◉ Limit by prefix                      │
│  Prefix:              archive/                                │
│  Actions:             ☑ Expire current versions               │
│  Days after creation: 2                                       │
│                                                               │
│                [ Create rule ]                                │
│                                                               │
│  Rule 2: Purge Athena results                                 │
│  Rule name:           purge-athena-results-7-days             │
│  Prefix:              athena-results/                         │
│  Days after creation: 7                                       │
│                                                               │
│  Rule 3: Purge rejected records                               │
│  Rule name:           purge-rejected-30-days                  │
│  Prefix:              rejected/                               │
│  Days after creation: 30                                      │
└──────────────────────────────────────────────────────────────┘
```

---

### Console Step 5: Set Up Budget Alert

```
AWS Console → AWS Budgets → Create budget

┌──────────────────────────────────────────────────────────────┐
│  Budget setup:         ◉ Customized                          │
│  Budget type:          ◉ Cost budget                         │
│                                                               │
│  Budget name:          data-pipeline-monthly                  │
│  Period:               Monthly                                │
│  Budget amount:        $5.00                                  │
│                                                               │
│  Alerts:                                                      │
│  Alert 1: 50% of budget ($2.50) → email notification         │
│  Alert 2: 80% of budget ($4.00) → email notification         │
│  Alert 3: 100% of budget ($5.00) → email notification        │
│                                                               │
│  Email recipients:     your-email@example.com                 │
│                                                               │
│                [ Create budget ]                              │
└──────────────────────────────────────────────────────────────┘
```

---

### Console Step 6: Verify Setup Checklist

Before moving to Phase 2, confirm everything exists:

```
□ S3 bucket created with 10 folder prefixes
□ S3 default encryption set to SSE-KMS with your CMK
□ S3 Block Public Access enabled
□ S3 lifecycle: 3 rules (archive/2d, athena-results/7d, rejected/30d)
□ KMS key created with alias "data-pipeline-key"
□ KMS key ARN noted down
□ IAM role: EMR_DefaultRole (with AmazonEMRServicePolicy_v2)
□ IAM role: EMR_EC2_DefaultRole (with S3, EMR, KMS inline policy)
□ EMR_EC2_DefaultRole added as KMS key user
□ Budget alert set at $5 with 3 thresholds
```

> [!IMPORTANT]
> **Save these values** — you'll need them in later phases:
> - S3 bucket name: `data-pipeline-dev-<ACCOUNT-ID>`
> - KMS Key ARN: `arn:aws:kms:eu-west-2:<ACCOUNT-ID>:key/<KEY-ID>`
> - EMR Service Role: `EMR_DefaultRole`
> - EMR EC2 Role: `EMR_EC2_DefaultRole`

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

# 6. Create IAM roles
aws iam create-role --role-name EMR_DefaultRole \
  --assume-role-policy-document file://infrastructure/iam_policies/emr_service_role.json
aws iam create-role --role-name EMR_EC2_DefaultRole \
  --assume-role-policy-document file://infrastructure/iam_policies/emr_ec2_role.json

# 7. Create budget
aws budgets create-budget --account-id <ACCOUNT-ID> \
  --budget file://infrastructure/budget_alert.json
```

> [!TIP]
> **Recommended approach:** Use the **Console** for Phase 1 (learning), then Phase 8 (Terraform) will codify everything for reproducibility. The JSON config files we create in this phase become the Terraform module inputs.
