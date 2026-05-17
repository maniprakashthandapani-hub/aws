# Phase 3 — Data Quality Framework (`data_quality.py`)

## Why This Phase Third?

Phase 2 built the ETL pipeline, but **how do you know the output is correct?** Without data quality checks, you could silently write corrupt, incomplete, or duplicate data to the processed zone — and downstream consumers (analysts, dashboards, ML models) would produce wrong results without anyone knowing.

Data quality is the **trust layer** between your pipeline and everyone who consumes the data.

---

## The DQ Philosophy: Fail Fast, Fail Loud

```
                    ┌──────────────────┐
     Raw CSV ──────▶│  PRE-PROCESSING  │──── Pass ────▶ Transform
                    │     CHECKS       │
                    └────────┬─────────┘
                             │
                          Fail? ──▶ ABORT immediately
                                   (don't waste compute on bad data)

                    ┌──────────────────┐
   Transformed ────▶│ POST-PROCESSING  │──── Pass ────▶ Write to S3
   DataFrame        │     CHECKS       │
                    └────────┬─────────┘
                             │
                          Fail? ──▶ ABORT before write
                                   (don't pollute processed zone)
```

**Why two checkpoints?**
- **Pre-processing** catches input problems BEFORE you spend compute transforming bad data
- **Post-processing** catches transformation bugs BEFORE they reach consumers

---

## Pre-Processing Checks (Before Transformation)

These run immediately after reading the CSV and validating the schema.

### Check 1: Empty File Detection

```python
def check_not_empty(df):
    """Reject empty files — nothing to process."""
    row_count = df.count()
    if row_count == 0:
        return DQResult(
            check="empty_file",
            status="SKIP",      # Not a failure — just no data today
            message="Input file has 0 rows. Marking DAG as success (no-op)."
        )
    return DQResult(check="empty_file", status="PASS", row_count=row_count)
```

**Action on failure:** `SKIP` — log it, mark the Airflow task as success. Empty files on some days may be normal (e.g., weekends, holidays).

**Who cares?**
- **App Maintenance** — knows the pipeline didn't fail, there was just no data
- **Product Owner** — no false alarms on empty-data days

---

### Check 2: Schema Match (Delegated to Phase 2's `schema_validator.py`)

Already covered in Phase 2. Called here as part of the DQ pipeline:
```python
is_valid, mismatches = validate_schema(df, schema_path)
if not is_valid:
    return DQResult(check="schema", status="ABORT", errors=mismatches)
```

---

### Check 3: Duplicate Row Detection

```python
def check_duplicates(df, key_columns=["customer_id", "transaction_date"]):
    """
    Detect and count full-duplicate rows.
    Action: DEDUPLICATE (remove dupes, log count).
    """
    total = df.count()
    deduped = df.dropDuplicates(key_columns)
    dupes_removed = total - deduped.count()
    
    return DQResult(
        check="duplicates",
        status="WARN" if dupes_removed > 0 else "PASS",
        message=f"Removed {dupes_removed} duplicate rows out of {total}",
        cleaned_df=deduped
    )
```

**Why deduplicate on key columns, not all columns?**
- Two rows with the same `customer_id` + `transaction_date` but different `amount` = possible data error
- Two rows identical in every column = definite duplicate from source
- We use key columns to catch both scenarios

---

### Check 4: Date Column Parseability

```python
def check_date_parseable(df, date_column="transaction_date"):
    """
    After date standardisation (Phase 2), check how many rows
    still have NULL in the parsed date column.
    """
    unparseable = df.filter(col(f"{date_column}_parsed").isNull()).count()
    total = df.count()
    pct = (unparseable / total) * 100
    
    if pct > 5.0:  # More than 5% unparseable — something is very wrong
        return DQResult(check="date_parse", status="ABORT",
            message=f"{pct:.1f}% of dates unparseable ({unparseable}/{total})")
    elif unparseable > 0:
        return DQResult(check="date_parse", status="WARN",
            message=f"{unparseable} rows with unparseable dates → quarantined")
    return DQResult(check="date_parse", status="PASS")
```

**Threshold is configurable** via `config/dq_thresholds.json`:
```json
{
  "date_parse_failure_abort_pct": 5.0,
  "null_pct_abort_threshold": 20.0,
  "min_expected_row_count": 1
}
```

---

### Check 5: Null Percentage per Column

```python
def check_null_percentage(df, thresholds):
    """
    For each column, check if null% exceeds the configured threshold.
    
    Example thresholds:
      customer_id: 0%   (any nulls = ABORT)
      email: 50%        (up to 50% null is acceptable)
      amount: 10%       (more than 10% null = WARN)
    """
    results = []
    total = df.count()
    
    for col_name, max_null_pct in thresholds.items():
        null_count = df.filter(col(col_name).isNull()).count()
        null_pct = (null_count / total) * 100
        
        if null_pct > max_null_pct:
            severity = "ABORT" if max_null_pct == 0 else "WARN"
            results.append(DQResult(
                check=f"null_pct_{col_name}",
                status=severity,
                message=f"{col_name}: {null_pct:.1f}% null (threshold: {max_null_pct}%)"
            ))
    return results
```

---

## Post-Processing Checks (After Transformation, Before Write)

These run on the final DataFrame, right before `df.write.parquet()`.

### Check 6: Output Row Count Sanity

```python
def check_output_count(df, input_count):
    """
    Output should have rows. A zero-row output after transformation
    means something went catastrophically wrong.
    """
    output_count = df.count()
    
    if output_count == 0:
        return DQResult(check="output_count", status="ABORT",
            message="Output has 0 rows — will NOT write empty Parquet")
    
    # Also check for suspicious data loss (>50% drop)
    drop_pct = ((input_count - output_count) / input_count) * 100
    if drop_pct > 50:
        return DQResult(check="output_count", status="WARN",
            message=f"Output lost {drop_pct:.1f}% of rows ({input_count}→{output_count})")
    
    return DQResult(check="output_count", status="PASS",
        message=f"Output: {output_count} rows (from {input_count} input)")
```

---

### Check 7: Mandatory Columns Not Null in Output

```python
def check_mandatory_not_null(df, mandatory_columns):
    """
    After all transformations, these columns MUST NOT have nulls.
    If they do, our null handling logic has a bug.
    """
    for col_name in mandatory_columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            return DQResult(check=f"mandatory_null_{col_name}", status="ABORT",
                message=f"BUG: {col_name} has {null_count} nulls after cleaning")
    
    return DQResult(check="mandatory_nulls", status="PASS")
```

---

### Check 8: Value Range Validation

```python
def check_value_ranges(df):
    """
    Business logic checks:
    - amount should be >= 0 (no negative transactions)
    - transaction_date should be within reasonable range
    """
    negative_amounts = df.filter(col("amount") < 0).count()
    future_dates = df.filter(col("dt") > current_date()).count()
    
    results = []
    if negative_amounts > 0:
        results.append(DQResult(check="negative_amount", status="WARN",
            message=f"{negative_amounts} rows with negative amount"))
    if future_dates > 0:
        results.append(DQResult(check="future_date", status="WARN",
            message=f"{future_dates} rows with future dates"))
    return results
```

---

## DQ Report Output

Every run generates a JSON report written to S3:

```json
{
  "run_id": "2026-05-18_daily_etl",
  "execution_date": "2026-05-18",
  "timestamp": "2026-05-18T06:30:45Z",
  "input_path": "s3://my-data-pipeline/landing/2026/05/18/",
  "input_row_count": 150000,
  "output_row_count": 149850,
  "checks": [
    {"check": "empty_file", "status": "PASS", "detail": "150000 rows"},
    {"check": "schema", "status": "PASS", "detail": "All 6 columns match"},
    {"check": "duplicates", "status": "WARN", "detail": "Removed 120 duplicates"},
    {"check": "date_parse", "status": "PASS", "detail": "0 unparseable dates"},
    {"check": "null_pct_customer_id", "status": "PASS", "detail": "0.0% null"},
    {"check": "null_pct_email", "status": "PASS", "detail": "12.3% null (threshold: 50%)"},
    {"check": "output_count", "status": "PASS", "detail": "149850 rows"},
    {"check": "mandatory_nulls", "status": "PASS", "detail": "All mandatory columns clean"},
    {"check": "negative_amount", "status": "PASS", "detail": "0 negative amounts"}
  ],
  "overall_status": "PASS",
  "rows_quarantined": 30,
  "quarantine_path": "s3://my-data-pipeline/rejected/2026/05/18/"
}
```

**Written to:** `s3://my-data-pipeline/logs/dq_reports/2026/05/18/dq_report.json`

**Who cares?**
- **Product Owner** — dashboard-ready metrics on data health
- **Data Consumer** — can check DQ report before trusting today's data
- **App Maintenance** — historical DQ trends show if data quality is degrading

---

## Quarantine Flow (Rejected Records)

```
Input: 150,000 rows
    │
    ├── 120 duplicates     → removed (logged in DQ report)
    ├── 30 unparseable dates → written to s3://…/rejected/YYYY/MM/DD/
    │
    └── 149,850 clean rows → written to s3://…/processed/dt=YYYY-MM-DD/
```

Rejected records are written as CSV (not Parquet) to `rejected/` so they can be:
1. Manually inspected by the data team
2. Fixed and re-submitted to `landing/`
3. Audited for source system issues

---

## The DQResult Object

```python
@dataclass
class DQResult:
    check: str          # Check name (e.g., "duplicates")
    status: str         # "PASS", "WARN", "ABORT", "SKIP"
    message: str = ""   # Human-readable detail
    cleaned_df: DataFrame = None  # Optional: cleaned DataFrame after fix
    
    @property
    def is_blocking(self):
        return self.status == "ABORT"
```

**Status levels:**
| Status | Meaning | Pipeline Action |
|--------|---------|----------------|
| `PASS` | Check passed | Continue |
| `WARN` | Non-critical issue | Log + continue |
| `ABORT` | Critical failure | Stop pipeline, raise error, alert via SNS |
| `SKIP` | Nothing to process | Mark success, no output |

---

## Configurable Thresholds (`config/dq_thresholds.json`)

```json
{
  "null_thresholds": {
    "customer_id": 0,
    "name": 10,
    "email": 50,
    "phone": 50,
    "transaction_date": 0,
    "amount": 10
  },
  "date_parse_failure_abort_pct": 5.0,
  "duplicate_abort_pct": 25.0,
  "output_drop_warn_pct": 50.0,
  "min_expected_row_count": 1
}
```

**Why configurable?**
- **Product Owner** can adjust thresholds without code changes
- Different environments (dev/staging/prod) can have different tolerances
- New columns can be added to null checks without modifying Python code

---

## Files Produced in This Phase

| File | Purpose |
|------|---------|
| `spark_jobs/data_quality.py` | Reusable DQ check module with 8 checks |
| `spark_jobs/config/dq_thresholds.json` | Configurable thresholds for DQ checks |
| `tests/test_data_quality.py` | Unit tests for each DQ check |

---

## Integration with Phase 2 (etl_main.py)

```python
# In etl_main.py — Step 5: Quality Checks
from data_quality import run_pre_checks, run_post_checks, write_dq_report

# Pre-processing DQ
pre_results, df_cleaned = run_pre_checks(df, schema_path, dq_config)
if any(r.is_blocking for r in pre_results):
    write_dq_report(pre_results, "ABORT")
    raise DataQualityException("Pre-processing DQ check failed")

# ... transformations ...

# Post-processing DQ
post_results = run_post_checks(df_final, input_count, dq_config)
if any(r.is_blocking for r in post_results):
    write_dq_report(pre_results + post_results, "ABORT")
    raise DataQualityException("Post-processing DQ check failed")

# All checks passed — write
write_dq_report(pre_results + post_results, "PASS")
df_final.coalesce(4).write.mode("overwrite").partitionBy("dt").parquet(output_path)
```

> [!IMPORTANT]
> The DQ report is written to S3 **regardless** of pass/fail. This creates an audit trail — you can always check what happened on any given day.
