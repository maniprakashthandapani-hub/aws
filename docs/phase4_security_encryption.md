# Phase 4 — Security & Encryption

## Why This Phase Fourth?

Phase 2 reads and transforms data. Phase 3 validates quality. But if the data contains **SPII (Sensitive Personally Identifiable Information)** — SSNs, emails, phone numbers — writing it as plaintext to S3 is a compliance violation waiting to happen.

Phase 4 adds **two layers of encryption**:
1. **Column-level** — encrypt SPII fields *inside* the DataFrame before writing
2. **File-level** — encrypt the entire Parquet file at rest in S3

This is the difference between "we store encrypted files" and "even if someone reads the file, they can't see the PII."

---

## The Two Encryption Layers Explained

```
┌─────────────────────────────────────────────────────────────────┐
│                    ENCRYPTION LAYERS                            │
│                                                                 │
│  Layer 1: COLUMN-LEVEL (Application)                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  DataFrame BEFORE encryption:                            │   │
│  │  ┌────────────┬──────────────────┬──────────────────┐   │   │
│  │  │ customer_id│ email            │ phone            │   │   │
│  │  ├────────────┼──────────────────┼──────────────────┤   │   │
│  │  │ 1001       │ john@email.com   │ +44-7700-900000  │   │   │
│  │  └────────────┴──────────────────┴──────────────────┘   │   │
│  │                                                          │   │
│  │  DataFrame AFTER encryption:                             │   │
│  │  ┌────────────┬──────────────────┬──────────────────┐   │   │
│  │  │ customer_id│ email_encrypted  │ phone_encrypted  │   │   │
│  │  ├────────────┼──────────────────┼──────────────────┤   │   │
│  │  │ 1001       │ a3f8b2c1...      │ 7d4e9f0a...      │   │   │
│  │  └────────────┴──────────────────┴──────────────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Layer 2: FILE-LEVEL (S3 SSE-KMS)                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  The entire Parquet file (including encrypted columns)   │   │
│  │  is encrypted again by S3 using the KMS key before       │   │
│  │  being stored on disk.                                   │   │
│  │                                                          │   │
│  │  S3 Console shows: 🔒 Server-side encryption: aws:kms   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Why Two Layers?

| Scenario | Column-Level Only | File-Level Only | Both ✅ |
|----------|-------------------|-----------------|---------|
| Someone with S3 access reads the Parquet | SPII is encrypted ✅ | All data visible ❌ | SPII encrypted ✅ |
| S3 bucket is accidentally made public | SPII is encrypted ✅ | Data exposed ❌ | SPII encrypted ✅ |
| Authorised analyst queries via Athena | Sees encrypted blobs (needs key to decrypt) | Sees all data including PII ❌ | Sees encrypted blobs for SPII ✅ |
| Backup/replication copies the file | SPII stays encrypted ✅ | Depends on destination encryption ⚠️ | Double protected ✅ |

---

## Column-Level Encryption: `encryption_utils.py`

### How It Works

```python
from pyspark.sql import functions as F
import boto3
import base64

class SPIIEncryptor:
    """
    Encrypts SPII columns using AES-256 with a data key from AWS KMS.
    
    Flow:
    1. Call KMS GenerateDataKey → returns plaintext key + encrypted key
    2. Use plaintext key with Spark's aes_encrypt() to encrypt columns
    3. Store the encrypted key in the Parquet metadata (for decryption later)
    4. Plaintext key is held in memory only — never written to disk/S3
    """
```

### Step-by-Step Encryption Flow

```
┌─────────────┐     GenerateDataKey      ┌─────────────┐
│  AWS KMS    │◀────────────────────────▶│  PySpark    │
│  (CMK)      │     Returns:             │  Driver     │
│             │     • plaintext_key       │             │
│             │     • encrypted_key       │             │
└─────────────┘                          └──────┬──────┘
                                                │
                                    Uses plaintext_key
                                    with aes_encrypt()
                                                │
                                         ┌──────▼──────┐
                                         │  Executors  │
                                         │  encrypt    │
                                         │  each row   │
                                         └──────┬──────┘
                                                │
                                    Writes Parquet with:
                                    • encrypted columns
                                    • encrypted_key in metadata
                                    • plaintext_key DISCARDED
                                                │
                                         ┌──────▼──────┐
                                         │  S3         │
                                         │  (SSE-KMS)  │
                                         └─────────────┘
```

### The Code

```python
def get_data_key(kms_key_arn, region="eu-west-2"):
    """
    Get a data encryption key from KMS.
    Returns plaintext key (for encryption) and encrypted key (for storage).
    """
    kms_client = boto3.client("kms", region_name=region)
    response = kms_client.generate_data_key(
        KeyId=kms_key_arn,
        KeySpec="AES_256"
    )
    return {
        "plaintext": base64.b64encode(response["Plaintext"]).decode(),
        "encrypted": base64.b64encode(response["CiphertextBlob"]).decode()
    }


def encrypt_spii_columns(df, columns, key_b64):
    """
    Encrypt specified columns using AES-256 (Spark built-in).
    
    Args:
        df: Input DataFrame
        columns: List of column names to encrypt (e.g., ["email", "phone", "ssn"])
        key_b64: Base64-encoded 256-bit AES key
    
    Returns:
        DataFrame with original columns replaced by encrypted versions
    """
    for col_name in columns:
        df = df.withColumn(
            f"{col_name}_encrypted",
            F.base64(F.expr(f"aes_encrypt({col_name}, unbase64('{key_b64}'), 'GCM')"))
        ).drop(col_name)  # Drop plaintext column
    
    return df


def decrypt_spii_columns(df, columns, key_b64):
    """
    Decrypt columns — used only by authorised consumers with KMS access.
    """
    for col_name in columns:
        encrypted_col = f"{col_name}_encrypted"
        df = df.withColumn(
            col_name,
            F.expr(f"CAST(aes_decrypt(unbase64({encrypted_col}), "
                   f"unbase64('{key_b64}'), 'GCM') AS STRING)")
        )
    return df
```

### Why AES-GCM Mode?

| Mode | Confidentiality | Integrity Check | Recommended |
|------|----------------|-----------------|-------------|
| ECB | ✅ | ❌ No tamper detection | ❌ Never use |
| CBC | ✅ | ❌ No tamper detection | ⚠️ Legacy only |
| **GCM** | ✅ | ✅ **Detects tampering** | ✅ **Use this** |

GCM (Galois/Counter Mode) provides both encryption AND authentication. If someone tampers with the encrypted data, decryption will fail with an error rather than silently returning garbage.

### Why Not Hardcode the Key?

```python
# ❌ NEVER DO THIS — key visible in source code, git history, logs
secret_key = "1234567890123456"
F.expr(f"aes_encrypt(email, '{secret_key}')")

# ✅ CORRECT — key fetched from KMS at runtime, never in code
data_key = get_data_key(kms_key_arn)  # From AWS KMS
F.expr(f"aes_encrypt(email, unbase64('{data_key['plaintext']}'), 'GCM')")
```

**Who cares?**
- **DevOps** — no secrets in git, no key rotation nightmares
- **Product Owner** — compliance with GDPR/data protection regulations
- **Data Consumer** — cannot accidentally see raw PII even if they have S3 access

---

## File-Level Encryption: S3 SSE-KMS

### How It Works

This is **transparent** — you don't write encryption code. It's configured via:

1. **Spark config** (in `spark-submit`):
```bash
--conf spark.hadoop.fs.s3a.server-side-encryption-algorithm=SSE-KMS
--conf spark.hadoop.fs.s3a.server-side-encryption.key=arn:aws:kms:eu-west-2:ACCOUNT:key/KEY-ID
```

2. **S3 bucket policy** (from Phase 1):
```json
{
  "Effect": "Deny",
  "Principal": "*",
  "Action": "s3:PutObject",
  "Resource": "arn:aws:s3:::my-data-pipeline/processed/*",
  "Condition": {
    "StringNotEquals": {
      "s3:x-amz-server-side-encryption": "aws:kms"
    }
  }
}
```

3. **S3 default encryption** (bucket setting):
```bash
aws s3api put-bucket-encryption --bucket my-data-pipeline \
  --server-side-encryption-configuration '{
    "Rules": [{"ApplyServerSideEncryptionByDefault": {
      "SSEAlgorithm": "aws:kms",
      "KMSMasterKeyID": "arn:aws:kms:eu-west-2:ACCOUNT:key/KEY-ID"
    }}]
  }'
```

### Three-Layer Defence

```
Layer 1: Bucket default encryption    → Encrypts everything by default
Layer 2: Bucket policy deny           → Rejects unencrypted uploads
Layer 3: Spark-submit config          → Spark explicitly uses SSE-KMS
```

If any one layer fails, the other two still protect the data.

---

## SPII Column Identification

Which columns are SPII? Defined in config, not hardcoded:

```json
// config/spii_columns.json
{
  "spii_columns": ["email", "phone", "ssn"],
  "encryption_mode": "GCM",
  "kms_key_arn": "arn:aws:kms:eu-west-2:ACCOUNT:key/KEY-ID"
}
```

**Why config-driven?**
- New SPII column discovered? Add to JSON, no code change
- Different environments may classify different columns as SPII
- Auditors can inspect the config to verify coverage

---

## Decryption: Who Can, and How

```
Data Consumer wants to see plaintext PII:
    │
    ├── Has KMS Decrypt permission? ──▶ NO ──▶ Sees encrypted blobs only
    │                                 
    └── YES ──▶ Calls KMS Decrypt to get data key
                    │
                    ▼
              Uses decrypt_spii_columns() to read plaintext
```

**Access control is at the KMS key level**, not the S3 level. This means:
- An analyst can query the Athena table but sees `a3f8b2c1...` for email
- Only authorised roles (e.g., compliance team) can decrypt

---

## Files Produced in This Phase

| File | Purpose |
|------|---------|
| `spark_jobs/encryption_utils.py` | SPII column encryption/decryption module |
| `spark_jobs/config/spii_columns.json` | Config: which columns to encrypt |
| `tests/test_encryption.py` | Unit tests for encrypt/decrypt round-trip |

---

## Integration with Phase 2 (etl_main.py)

```python
# In etl_main.py — Step 4: SPII Encryption
from encryption_utils import SPIIEncryptor

encryptor = SPIIEncryptor(kms_key_arn=config["kms_key_arn"])
df_encrypted = encryptor.encrypt_columns(df_transformed, config["spii_columns"])

# Original plaintext columns are DROPPED — only encrypted versions remain
# email → email_encrypted
# phone → phone_encrypted
# ssn   → ssn_encrypted
```

> [!CAUTION]
> The plaintext key exists only in Spark executor memory during the job. It is **never** written to S3, logs, or any persistent storage. Once the Spark job ends, the key is gone from memory.
