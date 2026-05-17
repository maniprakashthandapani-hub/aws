# Phase 6 — Operations, Monitoring & Maintenance

## Why This Phase Sixth?

Phases 1–5 built a working pipeline. But **building it is only 30% of the work**. The other 70% is keeping it running reliably, detecting problems before users do, and enabling the team to troubleshoot without you.

This phase answers the question every production system must answer: **"What happens at 3 AM when it breaks?"**

---

## The Four Personas and Their Concerns

This phase is designed from the perspective of everyone who interacts with this pipeline:

```
┌─────────────────────────────────────────────────────────────────┐
│                     WHO CARES ABOUT WHAT?                       │
│                                                                  │
│  DEVOPS                        APP MAINTENANCE                   │
│  ├── Is the cluster running?   ├── Did today's run succeed?    │
│  ├── Are we over budget?       ├── Why did task X fail?        │
│  ├── Are logs flowing to S3?   ├── How do I re-run yesterday? │
│  └── Is auto-terminate working?└── Where are the Spark logs?  │
│                                                                  │
│  PRODUCT OWNER                 DATA CONSUMER                    │
│  ├── Is data fresh (SLA met)?  ├── Is today's data available? │
│  ├── What's the DQ score?      ├── Can I trust the schema?    │
│  ├── How much are we spending? ├── How do I query this data?  │
│  └── Any PII exposure risk?    └── Why are some fields NULL?  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1. CloudWatch Alarms

### Alarm 1: EMR Cluster Idle Too Long

```json
{
  "AlarmName": "emr-cluster-idle-alert",
  "MetricName": "IsIdle",
  "Namespace": "AWS/ElasticMapReduce",
  "Statistic": "Average",
  "Period": 900,
  "EvaluationPeriods": 1,
  "Threshold": 1,
  "ComparisonOperator": "GreaterThanOrEqualToThreshold",
  "AlarmActions": ["arn:aws:sns:eu-west-2:ACCOUNT:data-pipeline-alerts"]
}
```

**What it detects:** A cluster sitting idle for >15 minutes (shouldn't happen — our job takes ~8 min).

**Why it matters:**
- Idle cluster = burning money for nothing
- Auto-terminate should catch this, but what if auto-terminate is misconfigured?
- This alarm is the safety net for the safety net

**Who cares:** DevOps, Product Owner (cost)

---

### Alarm 2: S3 Error Rate Spike

```json
{
  "AlarmName": "s3-error-rate-alert",
  "MetricName": "5xxErrors",
  "Namespace": "AWS/S3",
  "Statistic": "Sum",
  "Period": 300,
  "EvaluationPeriods": 1,
  "Threshold": 10,
  "ComparisonOperator": "GreaterThanThreshold",
  "AlarmActions": ["arn:aws:sns:eu-west-2:ACCOUNT:data-pipeline-alerts"]
}
```

**What it detects:** S3 returning server errors — potential AWS outage affecting our pipeline.

---

### Alarm 3: Budget Threshold Breach

```json
{
  "BudgetName": "data-pipeline-monthly",
  "BudgetLimit": {"Amount": "5", "Unit": "USD"},
  "Notifications": [
    {"Threshold": 50, "ThresholdType": "PERCENTAGE", "NotificationType": "ACTUAL"},
    {"Threshold": 80, "ThresholdType": "PERCENTAGE", "NotificationType": "ACTUAL"},
    {"Threshold": 100, "ThresholdType": "PERCENTAGE", "NotificationType": "ACTUAL"}
  ]
}
```

**What it detects:** Spending approaching the $5 safety limit.

**Alert progression:**
```
$2.50 spent (50%) → INFO email: "Heads up, halfway through budget"
$4.00 spent (80%) → WARNING email: "Approaching limit, review usage"
$5.00 spent (100%) → CRITICAL email: "Budget exceeded, take action"
```

**Who cares:** Product Owner, DevOps

---

## 2. SNS Alert Configuration

### Topic Setup

```bash
# Create SNS topic
aws sns create-topic --name data-pipeline-alerts --region eu-west-2

# Subscribe email
aws sns subscribe \
    --topic-arn arn:aws:sns:eu-west-2:ACCOUNT:data-pipeline-alerts \
    --protocol email \
    --notification-endpoint data-team@company.com
```

### What Triggers Alerts

| Event | Source | Severity | Action Required |
|-------|--------|----------|-----------------|
| DAG task failure (after retries) | Airflow callback | 🔴 Critical | Investigate + manual re-run |
| EMR cluster idle >15 min | CloudWatch alarm | 🟡 Warning | Check and terminate cluster |
| Budget at 80% | AWS Budgets | 🟡 Warning | Review remaining runs |
| Budget exceeded | AWS Budgets | 🔴 Critical | Pause DAG, audit costs |
| SLA miss (job not done by 8 AM) | Airflow SLA | 🟡 Warning | Check source file arrival |
| S3 5xx errors | CloudWatch alarm | 🟡 Warning | Check AWS status page |

---

## 3. Airflow SLA Configuration

```python
# In DAG definition
dag = DAG(
    dag_id="daily_emr_etl_pipeline",
    sla_miss_callback=sla_alert,
    ...
)

# On critical tasks
submit_step = EmrAddStepsOperator(
    task_id="submit_spark_step",
    sla=timedelta(hours=2),  # Must complete within 2 hours of scheduled time
    ...
)

def sla_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called when any task misses its SLA."""
    sns = SnsHook(aws_conn_id="aws_default")
    task_names = [t.task_id for t in task_list]
    sns.publish_to_target(
        target_arn=SNS_TOPIC_ARN,
        message=f"SLA MISS: Tasks {task_names} missed deadline",
        subject="⏰ ETL Pipeline SLA Miss",
    )
```

**Who cares:** Product Owner — data freshness commitment to stakeholders

---

## 4. S3 Lifecycle Rule for Archive Purge

```json
{
  "Rules": [
    {
      "ID": "purge-archive-after-2-days",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "archive/"
      },
      "Expiration": {
        "Days": 2
      }
    },
    {
      "ID": "cleanup-athena-results-7-days",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "athena-results/"
      },
      "Expiration": {
        "Days": 7
      }
    },
    {
      "ID": "cleanup-rejected-30-days",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "rejected/"
      },
      "Expiration": {
        "Days": 30
      }
    }
  ]
}
```

**Three retention policies:**
| Prefix | Retention | Rationale |
|--------|-----------|-----------|
| `archive/` | 2 days | Per requirement — original CSVs purged |
| `athena-results/` | 7 days | Query results are temporary |
| `rejected/` | 30 days | Rejected records kept for investigation |
| `processed/` | **No expiry** | Permanent data lake — this is the output |
| `logs/` | **No expiry** | Audit trail — keep for compliance |

---

## 5. Runbook — Common Failure Scenarios

### Scenario 1: Source File Not Found

```
SYMPTOM:  check_source_file task times out after 30 min
ROOT CAUSE: Upstream system didn't deliver the CSV
ACTION:
  1. Check with upstream team for delivery status
  2. If file is available, manually upload to s3://…/landing/YYYY/MM/DD/
  3. Trigger DAG re-run: airflow dags trigger daily_emr_etl_pipeline --exec-date YYYY-MM-DD
  4. If no file expected (holiday), mark task as success in Airflow UI
```

### Scenario 2: Spark Job OOM (Out of Memory)

```
SYMPTOM:  submit_spark_step fails with "Container killed by YARN for exceeding memory"
ROOT CAUSE: Data volume unexpectedly larger, or skewed partitions
ACTION:
  1. Check input file size: aws s3 ls s3://…/landing/YYYY/MM/DD/
  2. If file >2GB, increase executor memory:
     - Edit emr_config.py: executor-memory 4g → 6g
  3. If file is normal size, check for data skew in a specific column
  4. Re-run the DAG
```

### Scenario 3: EMR Cluster Creation Fails

```
SYMPTOM:  create_emr_cluster fails with "Insufficient capacity" or IAM error
ROOT CAUSE: Spot capacity exhausted, or IAM role missing permissions
ACTION:
  For Spot capacity:
    1. Wait 15 min and retry (Spot availability fluctuates)
    2. If persistent, switch core node to On-Demand temporarily
  For IAM error:
    1. Verify EMR_DefaultRole exists: aws iam get-role --role-name EMR_DefaultRole
    2. Check CloudTrail for Access Denied events
    3. Reapply IAM policies from infrastructure/iam_policies/
```

### Scenario 4: Schema Mismatch

```
SYMPTOM:  submit_spark_step fails with DataQualityException("schema")
ROOT CAUSE: Upstream changed the CSV format without notifying
ACTION:
  1. Download today's file: aws s3 cp s3://…/landing/YYYY/MM/DD/file.csv ./
  2. Compare headers with schema_definition.json
  3. If legitimate schema change:
     - Update schema_definition.json
     - Update etl_main.py if transformation logic affected
     - Re-run the DAG
  4. If upstream error:
     - Reject the file
     - Notify upstream team
```

### Scenario 5: Cluster Not Terminating

```
SYMPTOM:  CloudWatch alarm "emr-cluster-idle-alert" fires
ROOT CAUSE: terminate_cluster task failed, or DAG crashed before reaching it
ACTION:
  1. List active clusters: aws emr list-clusters --active
  2. Terminate manually: aws emr terminate-clusters --cluster-ids j-XXXXX
  3. Check Airflow logs for why terminate_cluster didn't run
  4. Verify trigger_rule="all_done" is set on terminate task
```

### Scenario 6: Data Quality Check Fails

```
SYMPTOM:  submit_spark_step fails with DataQualityException
ROOT CAUSE: Input data has quality issues beyond threshold
ACTION:
  1. Check DQ report: aws s3 cp s3://…/logs/dq_reports/YYYY/MM/DD/dq_report.json ./
  2. Review which check failed and the specific metrics
  3. Check rejected records: aws s3 ls s3://…/rejected/YYYY/MM/DD/
  4. If data is fixable: fix and reupload to landing/, re-run
  5. If threshold is too strict: adjust dq_thresholds.json, re-run
```

---

## 6. Log Accessibility Map

**"Where do I find logs for X?"**

| What You Need | Where to Find It | How to Access |
|---------------|-----------------|---------------|
| Airflow task logs | `s3://…/logs/airflow/daily_emr_etl/` | Airflow UI → Task → Log tab |
| Spark application logs | `s3://…/logs/emr/j-XXXXX/steps/` | EMR Console → Steps → Logs |
| Spark driver stdout | `s3://…/logs/emr/j-XXXXX/steps/s-XXXXX/stdout.gz` | `aws s3 cp + gunzip` |
| Spark driver stderr | `s3://…/logs/emr/j-XXXXX/steps/s-XXXXX/stderr.gz` | `aws s3 cp + gunzip` |
| YARN container logs | `s3://…/logs/emr/j-XXXXX/containers/` | EMR Console → Logs |
| DQ report | `s3://…/logs/dq_reports/YYYY/MM/DD/dq_report.json` | `aws s3 cp` |
| Athena query results | `s3://…/athena-results/` | Athena Console → History |
| CloudWatch metrics | CloudWatch Console | Filter by EMR namespace |
| CloudTrail (IAM audit) | CloudTrail Console | Filter by `EMR` or `S3` events |

---

## 7. Operational Checklist (Daily)

For the **App Maintenance** team:

```
□ Check Airflow UI — did today's DAG run succeed?
□ Check DQ report — any warnings to review?
□ Check S3 — does processed/dt=YYYY-MM-DD/ have files?
□ Check EMR Console — any active clusters? (should be 0)
□ Check budget — are we tracking within $5?
```

For the **DevOps** team (weekly):

```
□ Review CloudWatch alarms — any false positives to tune?
□ Check S3 storage growth — is lifecycle purge working?
□ Review Airflow resource usage — worker slots, scheduler lag
□ Audit IAM roles — any permission drift?
□ Check Spot pricing trends — still cost-effective?
```

---

## Files Produced in This Phase

| File | Purpose |
|------|---------|
| `infrastructure/cloudwatch_alarms.json` | Alarm definitions (idle cluster, S3 errors) |
| `infrastructure/sns_topic_setup.sh` | SNS topic creation and subscription script |
| `infrastructure/budget_alert.json` | AWS Budget configuration |
| `docs/runbook.md` | Failure scenarios and resolution steps |

---

## Integration Summary

```
Phase 6 ties everything together:

Phase 1 (Infra)    → CloudWatch monitors the infra
Phase 2 (ETL)      → DQ reports feed monitoring dashboards
Phase 3 (DQ)       → DQ failures trigger SNS alerts
Phase 4 (Security) → CloudTrail audits encryption compliance
Phase 5 (Airflow)  → SLA misses and failures trigger alerts
Phase 6 (THIS)     → Observability layer across all phases
Phase 7 (Athena)   → Query results cleaned up by lifecycle rules
```

> [!IMPORTANT]
> Monitoring is **not optional**. A pipeline without monitoring is a pipeline that fails silently. By the time someone notices, downstream reports have been wrong for days, and trust is lost.
