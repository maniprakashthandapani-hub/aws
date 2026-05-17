# Phase 2 — PySpark Core ETL (`etl_main.py`)

## Why This Phase Second?

With infrastructure in place (Phase 1), we now write the **core data processing logic**. This is the heart of the pipeline — the PySpark code that reads, validates, transforms, and writes data. Every other phase (DQ, encryption, Airflow) wraps around or extends this code.

---

## What We're Building

### The ETL Flow (Step by Step)

```
┌──────────────────────────────────────────────────────────────────┐
│                        etl_main.py                               │
│                                                                  │
│  Step 1: READ          Read CSV from s3://…/landing/YYYY/MM/DD/ │
│          │             with explicit schema (no inferSchema)     │
│          ▼                                                       │
│  Step 2: VALIDATE      Compare against schema_definition.json   │
│          │             → ABORT if mismatch                       │
│          ▼                                                       │
│  Step 3: CLEAN         Handle nulls (drop/fill/flag per column) │
│          │             Standardise dates → yyyy-MM-dd            │
│          │             Remove full duplicates                    │
│          ▼                                                       │
│  Step 4: TRANSFORM     Business logic (derived columns, etc.)   │
│          │             Encrypt SPII columns (Phase 4 module)     │
│          ▼                                                       │
│  Step 5: QUALITY       Pre-write DQ checks (Phase 3 module)     │
│          │             → ABORT if critical check fails           │
│          ▼                                                       │
│  Step 6: WRITE         Parquet → s3://…/processed/dt=YYYY-MM-DD │
│          │             coalesce(4) for optimal file sizing       │
│          ▼                                                       │
│  Step 7: ARCHIVE       Move source CSV → s3://…/archive/        │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

### 1. `etl_main.py` — The Main Entry Point

**What it does:** Orchestrates the entire ETL flow from read to write.

**Key design decisions:**

#### a) Explicit Schema (No `inferSchema`)

```python
# ❌ BAD — inferSchema reads the file TWICE (once to infer, once to read)
df = spark.read.option("inferSchema", "true").csv(input_path)

# ✅ GOOD — explicit schema reads once, catches type drift immediately
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=True),
    StructField("phone", StringType(), nullable=True),
    StructField("transaction_date", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
])

df = spark.read.schema(schema).option("header", "true").csv(input_path)
```

**Why explicit schema matters:**
| Aspect | `inferSchema=true` | Explicit Schema |
|--------|-------------------|-----------------|
| Performance | Reads file **twice** | Reads file **once** |
| Type safety | Guesses types (may be wrong) | Enforced types (catches drift) |
| Cost | 2x S3 GET charges | 1x S3 GET charges |
| Nullability | Cannot enforce | Enforced per column |

**Who cares?**
- **Data Consumer** — guaranteed column types, no surprise `StringType` where `IntegerType` expected
- **App Maintenance** — schema drift caught at read time, not downstream

---

#### b) Date Standardisation

Source CSVs may have mixed date formats:
```
"17/05/2026"     → dd/MM/yyyy
"2026-05-17"     → yyyy-MM-dd
"May 17, 2026"   → MMM dd, yyyy
"05-17-2026"     → MM-dd-yyyy
```

**Our approach — cascading parse with fallback:**
```python
from pyspark.sql.functions import to_date, coalesce, col, lit

df = df.withColumn("transaction_date_parsed",
    coalesce(
        to_date(col("transaction_date"), "yyyy-MM-dd"),
        to_date(col("transaction_date"), "dd/MM/yyyy"),
        to_date(col("transaction_date"), "MM-dd-yyyy"),
        to_date(col("transaction_date"), "MMM dd, yyyy"),
    )
)

# Flag unparseable dates (will be NULL after coalesce)
df = df.withColumn("date_parse_failed",
    col("transaction_date_parsed").isNull()
)
```

**Output:** All dates stored as `yyyy-MM-dd` (ISO 8601). Unparseable rows flagged for quarantine.

---

#### c) Null Handling Strategy

Nulls are handled **per column** based on a config, not globally:

| Column | Null Strategy | Rationale |
|--------|--------------|-----------|
| `customer_id` | **DROP** row | Primary key — row is meaningless without it |
| `name` | **FILL** with `"Unknown"` | Non-critical, but needed for display |
| `email` | **FLAG** as `is_email_null=true` | Optional field — keep row, mark absence |
| `phone` | **FLAG** as `is_phone_null=true` | Optional field — keep row, mark absence |
| `transaction_date` | **QUARANTINE** row | Date is critical for partitioning |
| `amount` | **FILL** with `0.0` | Default to zero, flag as `is_amount_defaulted=true` |

**Implementation:**
```python
# Drop rows where customer_id is null
df = df.dropna(subset=["customer_id"])

# Fill with defaults
df = df.fillna({"name": "Unknown", "amount": 0.0})

# Flag nulls before filling
df = df.withColumn("is_email_null", col("email").isNull())
df = df.withColumn("is_phone_null", col("phone").isNull())
```

**Why not just `dropna()` on everything?**
- Loses data unnecessarily — an order with missing email is still a valid order
- Business rules differ per column — only the **Product Owner** can decide what's critical

---

#### d) Idempotent Write

```python
# Overwrite the specific date partition — not the entire table
df_final.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .option("path", "s3://my-data-pipeline/processed/") \
    .format("parquet") \
    .save()
```

**Why `mode("overwrite")` with `partitionBy("dt")`?**
- Re-running the same date replaces ONLY that day's partition
- Other dates are untouched
- No duplicates — ever
- Safe for Airflow retries and backfills

---

#### e) `coalesce(4)` Before Write

```python
# ❌ BAD — 200 tiny files (default shuffle partitions)
df_final.write.parquet(output_path)

# ✅ GOOD — 4 well-sized files (~75MB each for 1GB input)
df_final.coalesce(4).write.parquet(output_path)
```

**Why 4 files?**
- 1GB CSV → ~300MB Parquet (Snappy) → 4 × 75MB
- Athena performs best with files between 64–128MB
- Too many small files = S3 LIST overhead + slow queries
- Too few large files = no parallelism in reads

---

### 2. `schema_validator.py` — Schema Contract Enforcement

**What it does:** Compares the DataFrame's schema against a JSON contract file.

```python
def validate_schema(df, expected_schema_path):
    """
    Returns: (is_valid: bool, mismatches: list[str])
    
    Checks:
    1. All expected columns exist
    2. No unexpected columns present
    3. Data types match
    4. Nullability constraints respected
    """
```

**The schema contract (`config/schema_definition.json`):**
```json
{
  "columns": [
    {"name": "customer_id", "type": "integer", "nullable": false},
    {"name": "name", "type": "string", "nullable": false},
    {"name": "email", "type": "string", "nullable": true},
    {"name": "phone", "type": "string", "nullable": true},
    {"name": "transaction_date", "type": "string", "nullable": false},
    {"name": "amount", "type": "double", "nullable": false}
  ]
}
```

**What happens on mismatch:**
```
Schema Validation ──▶ Missing column?     ──▶ ABORT + log error
                  ──▶ Extra column?       ──▶ WARN + log (schema evolution)
                  ──▶ Type mismatch?      ──▶ ABORT + log error
                  ──▶ Nullable violation? ──▶ ABORT + log error
```

**Who cares?**
- **Data Consumer** — their downstream queries won't break from surprise column changes
- **App Maintenance** — schema drift caught at pipeline start, not after 10 mins of processing
- **Product Owner** — data contract honoured between producer and consumer

---

### 3. Sample CSV (`data/sample/sample_input.csv`)

A realistic test file with intentional edge cases:
- Mixed date formats
- Null values in various columns
- Duplicate rows
- Special characters in name field
- Valid SPII data (fake SSN, email, phone)

This lets us test the full pipeline locally before touching AWS.

---

## Files Produced in This Phase

| File | Purpose |
|------|---------|
| `spark_jobs/etl_main.py` | Main PySpark entry point — full ETL flow |
| `spark_jobs/schema_validator.py` | Schema contract validation module |
| `spark_jobs/config/schema_definition.json` | Expected schema contract |
| `spark_jobs/config/spark_tuning.json` | Spark-submit configuration |
| `data/sample/sample_input.csv` | Test CSV with edge cases |
| `docs/spark_tuning_guide.md` | Spark memory math documentation |

---

## Dependencies on Other Phases

```
Phase 1 (Infrastructure) ──▶ S3 paths, KMS key ARN, IAM roles
Phase 2 (THIS PHASE)     ──▶ Core ETL logic
Phase 3 (Data Quality)   ──▶ DQ module called from Step 5 of etl_main.py
Phase 4 (Encryption)     ──▶ Encryption module called from Step 4 of etl_main.py
```

> [!NOTE]
> Phase 2 creates **placeholder imports** for Phase 3 (data_quality) and Phase 4 (encryption_utils). These modules are built in their respective phases but are called from `etl_main.py`.
