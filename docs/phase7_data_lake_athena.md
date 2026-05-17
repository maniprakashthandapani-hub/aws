# Phase 7 — Data Lake & Athena Query Layer

## Why This Phase Seventh?

Phases 1–6 built a pipeline that reads CSV, transforms it, writes Parquet, and monitors everything. But **who consumes the output?** A Parquet file sitting in S3 is useless unless someone can query it.

Phase 7 turns your S3 `processed/` folder into a **queryable data lake** — analysts, dashboards, and downstream systems can run SQL directly against your Parquet files using Amazon Athena, without needing Spark, EMR, or any infrastructure.

---

## What Is a Data Lake (vs. Data Warehouse)?

```
DATA WAREHOUSE (e.g., Redshift)         DATA LAKE (S3 + Glue + Athena)
──────────────────────────              ─────────────────────────────
• Data must be LOADED first             • Data stays in S3 as-is
• Cluster runs 24/7 (costs $$$)         • No cluster — serverless queries
• Fixed schema at load time             • Schema-on-read (flexible)
• Fast for complex joins                • Fast for scans and aggregations
• $0.25/hr minimum                      • $5/TB scanned (pay per query)
                                        
For our MVP with <1GB/day:              ✅ CORRECT CHOICE
$180/month minimum                      ~$0.002/month at our scale
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      DATA LAKE STACK                          │
│                                                               │
│  ┌────────────┐    ┌─────────────────┐    ┌──────────────┐  │
│  │ S3 Bucket  │    │  Glue Catalog   │    │   Athena     │  │
│  │ processed/ │◀──▶│  (Metadata)     │◀──▶│  (SQL Engine)│  │
│  │ dt=YYYY-.. │    │                 │    │              │  │
│  └────────────┘    │  Database:      │    │  Workgroup:  │  │
│                    │  data_lake_db   │    │  pipeline-wg │  │
│  Stores the        │                 │    │              │  │
│  actual data       │  Table:         │    │  Scans only  │  │
│  (Parquet files)   │  processed_data │    │  needed cols │  │
│                    │                 │    │  & partitions│  │
│                    │  Partitions:    │    │              │  │
│                    │  dt=2026-05-18  │    │  Results →   │  │
│                    │  dt=2026-05-19  │    │  s3://…/     │  │
│                    │  dt=2026-05-20  │    │  athena-     │  │
│                    │  ...            │    │  results/    │  │
│                    └─────────────────┘    └──────────────┘  │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

**How it works:**
1. **S3** stores the Parquet files (your data)
2. **Glue Catalog** stores the *metadata* (schema, partitions, S3 location)
3. **Athena** reads the metadata from Glue, then queries S3 directly

Athena never copies your data. It reads Parquet from S3 on every query.

---

## Step-by-Step: Manual AWS Console Setup

### Step 1: Create Glue Database

```
AWS Console → AWS Glue → Databases → Add database

┌──────────────────────────────────────────────┐
│  Create a database                            │
│                                               │
│  Database name:    data_lake_db               │
│  Location:         (leave blank)              │
│  Description:      Data lake for ETL pipeline │
│                    processed output            │
│                                               │
│              [ Create database ]              │
└──────────────────────────────────────────────┘
```

**CLI equivalent:**
```bash
aws glue create-database --database-input '{
  "Name": "data_lake_db",
  "Description": "Data lake for ETL pipeline processed output"
}' --region eu-west-2
```

---

### Step 2: Create Glue Table

```
AWS Console → AWS Glue → Tables → Add table manually

┌──────────────────────────────────────────────┐
│  Add a table                                  │
│                                               │
│  Table name:       processed_data             │
│  Database:         data_lake_db               │
│                                               │
│  Data store:       S3                         │
│  S3 path:          s3://my-data-pipeline-     │
│                    ACCOUNT/processed/          │
│  Data format:      Parquet                    │
│  Classification:   parquet                    │
│                                               │
│  Define schema:                               │
│  ┌────────────────┬──────────┬──────────┐    │
│  │ Column Name     │ Type     │ Comment  │    │
│  ├────────────────┼──────────┼──────────┤    │
│  │ customer_id     │ int      │ PK       │    │
│  │ name            │ string   │          │    │
│  │ email_encrypted │ string   │ AES-GCM  │    │
│  │ phone_encrypted │ string   │ AES-GCM  │    │
│  │ amount          │ double   │          │    │
│  │ is_email_null   │ boolean  │ flag     │    │
│  │ is_phone_null   │ boolean  │ flag     │    │
│  └────────────────┴──────────┴──────────┘    │
│                                               │
│  Partition keys:                              │
│  ┌────────────────┬──────────┐               │
│  │ Partition Name  │ Type     │               │
│  ├────────────────┼──────────┤               │
│  │ dt              │ string   │               │
│  └────────────────┴──────────┘               │
│                                               │
│              [ Create table ]                 │
└──────────────────────────────────────────────┘
```

**CLI equivalent:**
```bash
aws glue create-table --database-name data_lake_db --table-input '{
  "Name": "processed_data",
  "StorageDescriptor": {
    "Columns": [
      {"Name": "customer_id", "Type": "int"},
      {"Name": "name", "Type": "string"},
      {"Name": "email_encrypted", "Type": "string"},
      {"Name": "phone_encrypted", "Type": "string"},
      {"Name": "amount", "Type": "double"},
      {"Name": "is_email_null", "Type": "boolean"},
      {"Name": "is_phone_null", "Type": "boolean"}
    ],
    "Location": "s3://my-data-pipeline-ACCOUNT/processed/",
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "SerdeInfo": {
      "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
  },
  "PartitionKeys": [
    {"Name": "dt", "Type": "string"}
  ],
  "TableType": "EXTERNAL_TABLE"
}' --region eu-west-2
```

---

### Step 3: Create Athena Workgroup

```
AWS Console → Amazon Athena → Workgroups → Create workgroup

┌──────────────────────────────────────────────┐
│  Create workgroup                             │
│                                               │
│  Workgroup name:   data-pipeline-wg           │
│                                               │
│  Query result location:                       │
│  s3://my-data-pipeline-ACCOUNT/athena-results/│
│                                               │
│  ☑ Override client-side settings              │
│    (forces all queries to use this location)  │
│                                               │
│  Per-query data usage control:                │
│  Maximum:  100 MB                             │
│  Action:   Cancel query                       │
│                                               │
│  Per-workgroup data usage control:            │
│  Maximum:  1 GB per day                       │
│  Action:   Alert via SNS                      │
│                                               │
│  ☑ Publish metrics to CloudWatch              │
│                                               │
│  Encryption:                                  │
│  ☑ Encrypt query results                      │
│  Encryption type: SSE-KMS                     │
│  KMS key: alias/data-pipeline-key             │
│                                               │
│              [ Create workgroup ]             │
└──────────────────────────────────────────────┘
```

**Why cost controls on the workgroup?**
- Prevents accidental `SELECT * FROM processed_data` scanning all data ($5/TB!)
- 100MB per-query limit: at our scale (<1GB total), this is generous
- 1GB daily limit: safety net for runaway queries or scripts

**CLI equivalent:**
```bash
aws athena create-work-group --name data-pipeline-wg \
  --configuration '{
    "ResultConfiguration": {
      "OutputLocation": "s3://my-data-pipeline-ACCOUNT/athena-results/",
      "EncryptionConfiguration": {
        "EncryptionOption": "SSE_KMS",
        "KmsKey": "arn:aws:kms:eu-west-2:ACCOUNT:alias/data-pipeline-key"
      }
    },
    "EnforceWorkGroupConfiguration": true,
    "BytesScannedCutoffPerQuery": 104857600,
    "PublishCloudWatchMetricsEnabled": true
  }' --region eu-west-2
```

---

### Step 4: Register Partitions (After First Pipeline Run)

Once the pipeline writes its first Parquet file, register the partition:

```
AWS Console → Amazon Athena → Query editor

SELECT database: data_lake_db

Run:
  MSCK REPAIR TABLE processed_data;

Expected output:
  Partitions found: dt=2026-05-18
```

**This is automated in the DAG** (Phase 5: `repair_partitions` task), but you'll need to run it manually the first time to verify the setup works.

---

### Step 5: Run Test Queries

```sql
-- 1. Verify table structure
DESCRIBE data_lake_db.processed_data;

-- 2. Count rows per partition
SELECT dt, COUNT(*) as row_count
FROM data_lake_db.processed_data
GROUP BY dt
ORDER BY dt;

-- 3. Check encrypted columns (should see base64 blobs, not plaintext)
SELECT customer_id, name, email_encrypted, phone_encrypted
FROM data_lake_db.processed_data
WHERE dt = '2026-05-18'
LIMIT 5;

-- 4. Data quality audit via Athena
SELECT dt,
       COUNT(*) as total_rows,
       SUM(CASE WHEN is_email_null THEN 1 ELSE 0 END) as null_emails,
       SUM(CASE WHEN is_phone_null THEN 1 ELSE 0 END) as null_phones,
       AVG(amount) as avg_amount,
       MIN(amount) as min_amount,
       MAX(amount) as max_amount
FROM data_lake_db.processed_data
GROUP BY dt;

-- 5. Cross-day comparison
SELECT dt,
       COUNT(DISTINCT customer_id) as unique_customers,
       SUM(amount) as total_amount
FROM data_lake_db.processed_data
GROUP BY dt
ORDER BY dt;
```

---

## Hive-Style Partitioning Explained

**What PySpark writes:**
```
s3://my-data-pipeline/processed/
├── dt=2026-05-18/
│   ├── part-00000.snappy.parquet    (~75 MB)
│   ├── part-00001.snappy.parquet    (~75 MB)
│   ├── part-00002.snappy.parquet    (~75 MB)
│   └── part-00003.snappy.parquet    (~75 MB)
├── dt=2026-05-19/
│   ├── part-00000.snappy.parquet
│   └── ...
```

**What Athena sees:**
```
Table: processed_data
├── Partition dt=2026-05-18  →  scans 4 files (~300 MB)
├── Partition dt=2026-05-19  →  scans 4 files (~300 MB)
└── ...
```

**Why this matters for cost:**
```sql
-- This query scans ALL partitions (~300MB × 7 days = 2.1 GB)
SELECT * FROM processed_data;
-- Cost: $0.01

-- This query scans ONLY 1 partition (~300 MB)
SELECT * FROM processed_data WHERE dt = '2026-05-18';
-- Cost: $0.0015
```

**Rule:** Always include `WHERE dt = '...'` in your queries to avoid full-table scans.

---

## Glue Crawler vs. MSCK REPAIR TABLE

| Aspect | Glue Crawler | MSCK REPAIR TABLE |
|--------|-------------|-------------------|
| **Cost** | $0.44/DPU-hour (min 10 min) | Free (Athena DDL query) |
| **How it works** | Scans S3, infers schema, adds partitions | Scans S3, adds partitions (schema must exist) |
| **Schema discovery** | ✅ Auto-discovers schema | ❌ Schema must be pre-defined |
| **Speed** | Slower (spins up DPUs) | Faster (instant query) |
| **For our use case** | ❌ Overkill — we know the schema | ✅ **Use this** |

**We chose `MSCK REPAIR TABLE`** because:
- Schema is fixed (defined in Phase 2)
- Only new partitions need to be registered
- It's free — no DPU charges
- It's faster — runs as an Athena DDL query

---

## IAM for Data Consumers

Data consumers (analysts, BI tools) need a **separate IAM policy** from the EMR pipeline role:

```
Pipeline Role (EMR):                  Consumer Role (Analyst):
├── s3:GetObject on landing/          ├── s3:GetObject on processed/ ONLY
├── s3:PutObject on processed/        ├── athena:StartQueryExecution
├── s3:PutObject on archive/          ├── athena:GetQueryResults
├── kms:Encrypt + Decrypt             ├── glue:GetTable, GetPartitions
└── emr:* (cluster management)        ├── s3:PutObject on athena-results/
                                      └── kms:Decrypt (ONLY if authorised
                                          to see plaintext SPII)
```

**Key principle:** Consumers can READ processed data but CANNOT write to it, modify infrastructure, or access raw landing data.

---

## Files Produced in This Phase

| File | Purpose |
|------|---------|
| `infrastructure/glue_catalog_setup.json` | Glue database + table definition |
| `infrastructure/iam_policies/athena_glue_policy.json` | Consumer IAM policy |
| `athena/create_database.sql` | DDL for Glue database |
| `athena/create_table.sql` | DDL for Glue table with partitioning |
| `athena/sample_queries.sql` | Ready-to-run analytical queries |
| `athena/workgroup_setup.sh` | Athena workgroup creation script |

---

## Cost Impact

| Component | 7-Day Cost | Notes |
|-----------|------------|-------|
| Glue Catalog | **$0.00** | Free tier: 1M objects + 1M requests |
| Athena (10 queries/day × 300MB) | **$0.015** | $5/TB × 0.003 TB |
| S3 athena-results storage | **$0.001** | Tiny CSV results, purged after 7 days |
| **Total Phase 7 cost** | **~$0.02** | Negligible |

> [!TIP]
> Athena + Glue is the most cost-effective query layer for data under 10 GB. You pay literally fractions of a cent per query. No servers, no clusters, no maintenance.
