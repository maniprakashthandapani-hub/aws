# Phase 4 — Security & Encryption

## The Architecture: Dual-Layer Encryption

Data security in the cloud requires defense-in-depth. We have implemented a two-layered encryption strategy using AWS KMS.

### 1. Storage-Level Encryption (SSE-KMS)
- **What it is:** The entire S3 bucket enforces `aws:kms` encryption.
- **Where it happens:** This is handled by AWS infrastructure automatically when Spark writes the Parquet files to the `processed/` bucket. 
- **Implementation:** In Phase 1, we attached the S3 bucket policy denying unencrypted uploads, and in Phase 2, we added `--conf spark.hadoop.fs.s3a.server-side-encryption.key=<KMS_ARN>` to our Spark tuning config.

### 2. Column-Level Encryption (Envelope Encryption)
- **What it is:** Specific columns containing SPII (Sensitive Personal Identifiable Information) like `email` and `phone` are encrypted *before* the Parquet file is ever written to disk.
- **Where it happens:** Inside the EMR Serverless PySpark memory.
- **Implementation:** `spark_jobs/encryption_utils.py` uses the **Envelope Encryption Pattern**.

## Envelope Encryption in PySpark

AWS KMS API calls are strictly metered and throttled. If we called the KMS API for every single row in a 1-billion row dataset, the job would take days and cost thousands of dollars in API fees.

Instead, we use Envelope Encryption:
1. **Generate Data Key:** The Spark Driver uses `boto3` to call `kms:GenerateDataKey`. This returns a 256-bit AES key in two formats: Plaintext and Ciphertext.
2. **Encrypt Locally:** We take the Plaintext key and use Spark 3's extremely fast, native `aes_encrypt` function to encrypt the sensitive columns in memory using GCM mode.
3. **Discard Plaintext Key:** The plaintext key only exists in RAM during the Spark job.
4. **Store Ciphertext Key:** We take the Ciphertext key and save it to S3 (`s3://.../config/keys/dt=YYYY-MM-DD/data_key.json`).

```
KMS ──▶ (Plaintext Key, Ciphertext Key)
                 │              │
                 ▼              ▼
   Spark aes_encrypt()      save_to_s3()
                 │              │
                 ▼              ▼
       Encrypted Parquet    data_key.json
```

## How Data Consumers Decrypt

When an authorized data consumer (e.g., another Spark job) needs to read the email addresses:
1. They read the Parquet file (S3 handles the SSE-KMS decryption automatically).
2. They read the `data_key.json` file for that specific date partition.
3. They call `kms:Decrypt` and pass the Ciphertext key. 
4. If their IAM role is allowed to use the KMS CMK, KMS returns the Plaintext key.
5. They use Spark's `aes_decrypt()` on the column using the Plaintext key to reveal the emails.

*Note: For the MVP, we are not building the consumer decryption script, but the data is safely encrypted at rest using industry best practices!*
