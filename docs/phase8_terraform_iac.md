# Phase 8 — Infrastructure as Code (Terraform)

## Why This Phase Eighth?

Phases 1–7 were built and configured **manually** — clicking through the AWS console, running CLI commands, creating JSON configs. This works for learning and initial setup, but it has serious problems for any real team:

```
MANUAL SETUP PROBLEMS:
─────────────────────
1. "It works on my account" — no reproducibility
2. "Who changed the IAM policy?" — no audit trail
3. "Set it up in staging too" — hours of clicking
4. "Disaster recovery" — rebuild everything from memory?
5. "What exactly is deployed?" — nobody knows for sure
```

**Terraform solves all of these.** It declares your entire infrastructure in `.tf` files, tracks state, and can create/destroy everything with one command.

---

## What Terraform Does (vs. What It Doesn't)

```
TERRAFORM MANAGES:                    TERRAFORM DOES NOT MANAGE:
──────────────────                    ─────────────────────────
✅ S3 bucket + policies              ❌ PySpark code (etl_main.py)
✅ IAM roles + policies              ❌ Airflow DAG code
✅ KMS key + aliases                 ❌ Data in S3 (CSVs, Parquet)
✅ EMR security configuration       ❌ Athena query results
✅ Glue database + table             ❌ Spark job execution
✅ Athena workgroup                  ❌ Airflow scheduling
✅ SNS topic + subscriptions
✅ CloudWatch alarms
✅ Budget alerts
✅ S3 lifecycle rules
```

**Rule of thumb:** Terraform manages the **infrastructure** (the stage). Your application code (the actors) is deployed separately.

---

## Terraform Project Structure

```
aws_data_engineer/
└── terraform/
    ├── main.tf                 # Provider config, backend
    ├── variables.tf            # Input variables (region, account, etc.)
    ├── outputs.tf              # Output values (bucket name, key ARN, etc.)
    ├── terraform.tfvars        # Variable values (NOT committed to git)
    ├── .terraform.lock.hcl     # Dependency lock (committed)
    │
    ├── modules/
    │   ├── s3/
    │   │   ├── main.tf         # Bucket, policies, lifecycle
    │   │   ├── variables.tf
    │   │   └── outputs.tf
    │   │
    │   ├── iam/
    │   │   ├── main.tf         # Roles, policies, instance profiles
    │   │   ├── variables.tf
    │   │   └── outputs.tf
    │   │
    │   ├── kms/
    │   │   ├── main.tf         # CMK, alias, key policy
    │   │   ├── variables.tf
    │   │   └── outputs.tf
    │   │
    │   ├── emr/
    │   │   ├── main.tf         # Security config, cluster template
    │   │   ├── variables.tf
    │   │   └── outputs.tf
    │   │
    │   ├── glue/
    │   │   ├── main.tf         # Database, table, crawler
    │   │   ├── variables.tf
    │   │   └── outputs.tf
    │   │
    │   ├── athena/
    │   │   ├── main.tf         # Workgroup, named queries
    │   │   ├── variables.tf
    │   │   └── outputs.tf
    │   │
    │   └── monitoring/
    │       ├── main.tf         # SNS, CloudWatch, budgets
    │       ├── variables.tf
    │       └── outputs.tf
    │
    └── environments/
        ├── dev.tfvars          # Dev environment overrides
        └── prod.tfvars         # Prod environment overrides
```

**Why modules?**
- Each module is independently testable
- Modules can be reused across environments (dev/staging/prod)
- Changes to S3 don't risk breaking IAM (blast radius control)
- Team members can own specific modules

---

## Core Terraform Files Explained

### `main.tf` — Provider & Backend

```hcl
terraform {
  required_version = ">= 1.5.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  # State stored in S3 (not local) for team collaboration
  backend "s3" {
    bucket         = "my-terraform-state-ACCOUNT"
    key            = "data-pipeline/terraform.tfstate"
    region         = "eu-west-2"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "data-pipeline"
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = "data-engineering"
    }
  }
}

# Module calls
module "kms" {
  source = "./modules/kms"
  environment = var.environment
}

module "s3" {
  source      = "./modules/s3"
  environment = var.environment
  kms_key_arn = module.kms.key_arn
}

module "iam" {
  source      = "./modules/iam"
  environment = var.environment
  s3_bucket_arn = module.s3.bucket_arn
  kms_key_arn   = module.kms.key_arn
}

module "emr" {
  source              = "./modules/emr"
  environment         = var.environment
  s3_bucket_name      = module.s3.bucket_name
  emr_service_role    = module.iam.emr_service_role_arn
  emr_ec2_role        = module.iam.emr_ec2_instance_profile_arn
  kms_key_arn         = module.kms.key_arn
}

module "glue" {
  source         = "./modules/glue"
  environment    = var.environment
  s3_bucket_name = module.s3.bucket_name
}

module "athena" {
  source         = "./modules/athena"
  environment    = var.environment
  s3_bucket_name = module.s3.bucket_name
  kms_key_arn    = module.kms.key_arn
}

module "monitoring" {
  source        = "./modules/monitoring"
  environment   = var.environment
  alert_email   = var.alert_email
  budget_limit  = var.budget_limit
}
```

---

### `variables.tf` — Input Variables

```hcl
variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "eu-west-2"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "alert_email" {
  description = "Email for pipeline alerts"
  type        = string
}

variable "budget_limit" {
  description = "Monthly budget limit in USD"
  type        = number
  default     = 5
}

variable "archive_retention_days" {
  description = "Days to retain archived source files"
  type        = number
  default     = 2
}
```

---

### Key Module: `modules/s3/main.tf`

```hcl
resource "aws_s3_bucket" "data_pipeline" {
  bucket = "data-pipeline-${var.environment}-${data.aws_caller_identity.current.account_id}"
  
  tags = {
    Name = "data-pipeline-${var.environment}"
  }
}

# Enforce encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "encryption" {
  bucket = aws_s3_bucket.data_pipeline.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true  # Reduces KMS API calls (cost saving)
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "block_public" {
  bucket = aws_s3_bucket.data_pipeline.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle rules
resource "aws_s3_bucket_lifecycle_configuration" "lifecycle" {
  bucket = aws_s3_bucket.data_pipeline.id
  
  rule {
    id     = "purge-archive"
    status = "Enabled"
    filter { prefix = "archive/" }
    expiration { days = var.archive_retention_days }
  }
  
  rule {
    id     = "purge-athena-results"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
  }
  
  rule {
    id     = "purge-rejected"
    status = "Enabled"
    filter { prefix = "rejected/" }
    expiration { days = 30 }
  }
}

# Bucket policy — deny unencrypted uploads
resource "aws_s3_bucket_policy" "enforce_encryption" {
  bucket = aws_s3_bucket.data_pipeline.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyUnencryptedUploads"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:PutObject"
        Resource  = "${aws_s3_bucket.data_pipeline.arn}/processed/*"
        Condition = {
          StringNotEquals = {
            "s3:x-amz-server-side-encryption" = "aws:kms"
          }
        }
      }
    ]
  })
}

# Create folder structure (empty objects)
resource "aws_s3_object" "folders" {
  for_each = toset([
    "landing/", "processed/", "archive/", "rejected/",
    "logs/emr/", "logs/airflow/", "logs/dq_reports/",
    "scripts/", "config/", "athena-results/"
  ])
  
  bucket  = aws_s3_bucket.data_pipeline.id
  key     = each.value
  content = ""
}
```

---

## Manual Steps Before Terraform (One-Time Bootstrap)

Before running `terraform init`, you need TWO things that Terraform can't create for itself:

### Bootstrap Step 1: Create Terraform State Bucket

```
AWS Console → S3 → Create bucket

Bucket name:     my-terraform-state-ACCOUNT
Region:          eu-west-2
Versioning:      ☑ Enabled (protects against accidental state deletion)
Encryption:      SSE-S3 (default)
Public access:   ☑ Block ALL public access
```

**CLI:**
```bash
aws s3 mb s3://my-terraform-state-ACCOUNT --region eu-west-2
aws s3api put-bucket-versioning --bucket my-terraform-state-ACCOUNT \
  --versioning-configuration Status=Enabled
```

### Bootstrap Step 2: Create DynamoDB Lock Table

```
AWS Console → DynamoDB → Create table

Table name:      terraform-locks
Partition key:   LockID (String)
Table class:     DynamoDB Standard
Read/Write:      On-demand (pay per request)
```

**CLI:**
```bash
aws dynamodb create-table --table-name terraform-locks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region eu-west-2
```

**Why a lock table?**
- Prevents two people running `terraform apply` at the same time
- Without locking, concurrent applies can corrupt state

---

## Terraform Workflow

### First-Time Setup

```bash
# 1. Navigate to terraform directory
cd terraform/

# 2. Initialise — downloads AWS provider, configures backend
terraform init

# 3. Create variable values file (NOT committed to git)
cat > terraform.tfvars << EOF
aws_region    = "eu-west-2"
environment   = "dev"
alert_email   = "your-email@example.com"
budget_limit  = 5
EOF

# 4. Plan — shows what will be created (DRY RUN)
terraform plan -out=tfplan

# 5. Review the plan output carefully
# It will show something like:
#   Plan: 23 to add, 0 to change, 0 to destroy.

# 6. Apply — creates everything
terraform apply tfplan
```

### Day-to-Day Operations

```bash
# Make a change (e.g., increase budget limit)
# Edit terraform.tfvars or module code

# Preview what changes
terraform plan

# Apply changes
terraform apply

# See current state
terraform show

# Destroy everything (end of trial)
terraform destroy
```

---

## What Gets Created (Terraform Resources)

| Resource | Count | Terraform Resource Type |
|----------|-------|------------------------|
| S3 bucket | 1 | `aws_s3_bucket` |
| S3 encryption config | 1 | `aws_s3_bucket_server_side_encryption_configuration` |
| S3 public access block | 1 | `aws_s3_bucket_public_access_block` |
| S3 lifecycle rules | 1 | `aws_s3_bucket_lifecycle_configuration` |
| S3 bucket policy | 1 | `aws_s3_bucket_policy` |
| S3 folder objects | 10 | `aws_s3_object` |
| KMS key | 1 | `aws_kms_key` |
| KMS alias | 1 | `aws_kms_alias` |
| IAM roles | 3 | `aws_iam_role` |
| IAM policies | 4 | `aws_iam_policy` |
| IAM instance profile | 1 | `aws_iam_instance_profile` |
| EMR security config | 1 | `aws_emr_security_configuration` |
| Glue database | 1 | `aws_glue_catalog_database` |
| Glue table | 1 | `aws_glue_catalog_table` |
| Athena workgroup | 1 | `aws_athena_workgroup` |
| Athena named queries | 3 | `aws_athena_named_query` |
| SNS topic | 1 | `aws_sns_topic` |
| SNS subscription | 1 | `aws_sns_topic_subscription` |
| CloudWatch alarms | 2 | `aws_cloudwatch_metric_alarm` |
| Budget | 1 | `aws_budgets_budget` |
| **Total** | **~37** | |

---

## Mapping: Manual Phase → Terraform Module

| Manual Phase | What Was Created | Now Managed By |
|-------------|------------------|----------------|
| Phase 1: IAM policies | `emr_service_role.json`, `emr_ec2_role.json` | `modules/iam/` |
| Phase 1: S3 bucket | `s3_bucket_policy.json`, `s3_lifecycle_rules.json` | `modules/s3/` |
| Phase 1: KMS key | `kms_key_policy.json` | `modules/kms/` |
| Phase 1: EMR config | `emr_cluster_config.json` | `modules/emr/` |
| Phase 6: CloudWatch | `cloudwatch_alarms.json` | `modules/monitoring/` |
| Phase 6: SNS | `sns_topic_setup.sh` | `modules/monitoring/` |
| Phase 6: Budget | `budget_alert.json` | `modules/monitoring/` |
| Phase 7: Glue | `glue_catalog_setup.json` | `modules/glue/` |
| Phase 7: Athena | Workgroup (console) | `modules/athena/` |

---

## Import Existing Resources

If you've already created resources manually (Phases 1–7), Terraform can **import** them:

```bash
# Import existing S3 bucket
terraform import module.s3.aws_s3_bucket.data_pipeline my-data-pipeline-ACCOUNT

# Import existing KMS key
terraform import module.kms.aws_kms_key.pipeline_key KEY-ID

# Import existing IAM role
terraform import module.iam.aws_iam_role.emr_service EMR_DefaultRole

# Import existing Glue database
terraform import module.glue.aws_glue_catalog_database.data_lake data_lake_db
```

**After import:** Run `terraform plan` — it should show **no changes** (in-sync).

---

## Cost of Terraform Itself

| Component | Cost |
|-----------|------|
| Terraform CLI | **Free** (open source) |
| S3 state bucket | ~$0.001/month (tiny file) |
| DynamoDB lock table | ~$0.00/month (on-demand, few requests) |
| **Total** | **Essentially free** |

---

## `.gitignore` Updates for Terraform

```gitignore
# Terraform
terraform/.terraform/
terraform/*.tfstate
terraform/*.tfstate.backup
terraform/*.tfplan
terraform/terraform.tfvars      # Contains environment-specific values
terraform/.terraform.lock.hcl   # KEEP this — commit the lock file

# But DO commit:
# terraform/*.tf
# terraform/modules/**/*.tf
# terraform/environments/*.tfvars  (non-sensitive overrides)
```

---

## Files Produced in This Phase

| File | Purpose |
|------|---------|
| `terraform/main.tf` | Provider, backend, module composition |
| `terraform/variables.tf` | Input variable definitions |
| `terraform/outputs.tf` | Output values (ARNs, names) |
| `terraform/modules/s3/main.tf` | S3 bucket, policies, lifecycle |
| `terraform/modules/iam/main.tf` | Roles, policies, instance profiles |
| `terraform/modules/kms/main.tf` | CMK, alias, key policy |
| `terraform/modules/emr/main.tf` | Security config, cluster template |
| `terraform/modules/glue/main.tf` | Database, table definition |
| `terraform/modules/athena/main.tf` | Workgroup, named queries |
| `terraform/modules/monitoring/main.tf` | SNS, CloudWatch, budgets |
| `terraform/environments/dev.tfvars` | Dev environment config |

> [!WARNING]
> **Never commit `terraform.tfvars`** if it contains sensitive values (emails, account IDs). Use `environments/dev.tfvars` for non-sensitive overrides and pass secrets via environment variables or AWS Secrets Manager.
