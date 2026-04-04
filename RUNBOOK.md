# Streamline — Runbook

> Operational guide for running, maintaining, and troubleshooting the Streamline data platform.

---

## Table of Contents

1. [Jobs Overview](#1-jobs-overview)
2. [Daily Operations](#2-daily-operations)
3. [Deployment](#3-deployment)
4. [Backfill & Reprocessing](#4-backfill--reprocessing)
5. [Streaming Operations](#5-streaming-operations)
6. [Maintenance](#6-maintenance)
7. [Troubleshooting](#7-troubleshooting)

---

## 1. Jobs Overview

### 1.1 Job Registry

| Job | Type | Schedule | Triggered By |
|---|---|---|---|
| `data_generator_and_bronze_continous` | Continuous job | Manual start/stop | Manual |
| `streamline_dlt_pipeline` | DLT Pipeline | Continuous 24/7 | Manual start |
| `streamline_batch_orchestrator_job` | Orchestrator | Daily 2AM | Scheduled |
| `streamline_clickstream_pipeline` | Job | Not scheduled | Orchestrator |
| `streamline_payments_pipeline` | Job | Not scheduled | Orchestrator |
| `streamline_customers_pipeline` | Job | Not scheduled | Orchestrator |
| `streamline_orders_silver` | Job | Not scheduled | Orchestrator |
| `streamline_products_pipeline` | Job | Not scheduled | Orchestrator |
| `vacuum_all_tables` | Maintenance | Sunday 3AM | Scheduled |
| `reprocess_dlq` | Adhoc | Not scheduled | Manual |
| `reprocess_quarantine` | Adhoc | Not scheduled | Manual |

### 1.2 Orchestrator DAG

```
streamline_batch_orchestrator_job (2AM daily)
│
├── streamline_clickstream_pipeline   ──┐
├── streamline_payments_pipeline      ──┤
├── streamline_customers_pipeline     ──┤──► gold_quality_metrics
├── streamline_orders_silver          ──┤──► gold_customer_360
└── streamline_products_pipeline      ──┘──► gold_orders_daily_summary
```

All Silver jobs run in parallel. Gold jobs run after all Silver jobs succeed.

### 1.3 Notebooks per Job

| Job | Notebooks |
|---|---|
| `streamline_clickstream_pipeline` | `silver_clickstream.py` → `gold_funnel_metrics.py` |
| `streamline_payments_pipeline` | `silver_payments.py` → `gold_payment_success_rate.py` |
| `streamline_customers_pipeline` | `silver_customers_dim.py` |
| `streamline_orders_silver` | `silver_orders.py` |
| `streamline_products_pipeline` | `silver_products_dim.py` |
| `streamline_batch_orchestrator_job` | Calls all above via DAB dependencies and `gold.orders_daily_summary`, `gold.customer_360`, `gold.data_quality_metrics` |
| `data_generator_and_bronze_continous` | `data_generator.py` → `bronze_delta.py` |
| `streamline_dlt_pipeline` | `gold_dlt_realtime.py` |
| `vacuum_all_tables` | `vacuum.py` |
| `reprocess_dlq` | `reprocess_dlq.py` |
| `reprocess_quarantine` | `reprocess_quarantine.py` |

---

## 2. Daily Operations

### 2.1 Normal Day Checklist

```
1. Verify data_generator_and_bronze_continous is running
2. Check streamline_dlt_pipeline is active (DLT UI)
3. After 2AM — verify streamline_batch_orchestrator_job succeeded
4. Check pipeline_state table for any failed runs
```

```sql
SELECT pipeline_name, status, last_run_time, good_record_count, bad_record_count
FROM streamline.silver.pipeline_state
WHERE DATE(last_run_time) = current_date()
ORDER BY last_run_time DESC;
```

### 2.2 Check Data Quality

```sql
-- Quarantine trends
SELECT pipeline_date, pipeline_name, quarantined_records, quality_score
FROM streamline.gold.data_quality_metrics
ORDER BY pipeline_date DESC, pipeline_name;

-- Dead letter queue
SELECT topic, COUNT(*) as count, MAX(ingested_at) as latest
FROM streamline.bronze.dead_letter
GROUP BY topic
ORDER BY latest DESC;
```

---

## 3. Deployment

### 3.1 Branch Strategy

```
main      ← production (protected)
develop   ← integration (protected)
feature/* ← active development
```

Direct push to `develop` and `main` is blocked. All changes go through PRs.

### 3.2 Deploy to Dev

```bash
# Open PR to develop
# GitHub Actions runs automatically:
#   1. pytest tests/ -v
#   2. databricks bundle validate --target dev
# On merge:
#   databricks bundle deploy --target dev
```

### 3.3 Deploy to Prod

```bash
# Open PR from develop to main
# GitHub Actions runs:
#   1. pytest tests/ -v
#   2. databricks bundle validate --target prod
# On merge:
#   databricks bundle deploy --target prod
```

### 3.4 Manual Deploy (emergency)

```bash
databricks bundle deploy --target dev
databricks bundle deploy --target prod
```

### 3.5 Destroy Bundle

```bash
databricks bundle destroy --target dev
databricks bundle destroy --target prod
```

> **Note:** Never delete or modify DAB-managed jobs from the Databricks UI. Manual UI changes cause Terraform state mismatch. Use CLI pause for temporary stops.

---

## 4. Backfill & Reprocessing

### 4.1 Silver Backfill

Trigger from Jobs UI → select job → **Run with different parameters**:

| Widget | Format | Example |
|---|---|---|
| `start_datetime` | `YYYY-MM-DD HH:MM:SS` | `2026-03-13 00:00:00` |
| `end_datetime` | `YYYY-MM-DD HH:MM:SS` | `2026-03-13 23:59:59` |

Rules: both must be provided together or both empty. `start_datetime` must be less than `end_datetime`. `merge_to_delta` ensures idempotency — reprocessing the same range multiple times is safe.

### 4.2 Gold Backfill

| Widget | Format | Example |
|---|---|---|
| `start_date` | `YYYY-MM-DD` | `2026-03-13` |
| `end_date` | `YYYY-MM-DD` | `2026-03-13` |

### 4.3 DLQ Reprocessing

Run `reprocess_dlq` job from Jobs UI when upstream Kafka message format issues are fixed:

| Widget | Values | Example |
|---|---|---|
| `topic` | `orders` / `payments` / `clickstream` | `orders` |

Reads `bronze.dead_letter` for the specified topic, re-parses messages, writes successfully parsed records to the bronze table via merge.

### 4.4 Quarantine Reprocessing

Run `reprocess_quarantine` job from Jobs UI after fixing upstream data quality issues:

| Widget | Values | Example |
|---|---|---|
| `source` | `orders` / `payments` | `orders` |

Reads the quarantine table, runs the full quality check pipeline, writes records that now pass to Silver via merge, writes still-failing records back to quarantine with updated `rejection_reason`. Idempotent — safe to rerun.

### 4.5 Full Reprocess from Scratch

Only needed if Silver tables need to be rebuilt entirely:

```sql
-- Delete pipeline state for affected pipeline
DELETE FROM streamline.silver.pipeline_state
WHERE pipeline_name = 'bronze_to_silver_orders';

-- Run the job normally — no datetime widgets
-- Pipeline detects no state and full-scans Bronze
-- merge_to_delta handles duplicates
```

---

## 5. Streaming Operations

### 5.1 Start Streaming

```
1. Start data_generator_and_bronze_continous from Jobs UI
   Set widget: sleep_seconds = 30 (lower = more data = more credits)

2. Start streamline_dlt_pipeline from DLT UI → Start
```

### 5.2 Stop Streaming

```
1. Jobs UI → data_generator_and_bronze_continous → Cancel run
   Kafka producer closes cleanly via finally block

2. DLT UI → streamline_dlt_pipeline → Stop
```

### 5.3 Pause a Scheduled Job

```bash
# Pause via CLI — no state impact
databricks jobs update <job_id> --json '{"schedule": {"pause_status": "PAUSED"}}'

# Resume
databricks jobs update <job_id> --json '{"schedule": {"pause_status": "UNPAUSED"}}'
```

> **Warning:** Never uncouple a job from the bundle via the UI. Use CLI pause only.

### 5.4 Check Stream Health

```sql
SELECT DATE(ingested_at) as date, COUNT(*) as records
FROM streamline.bronze.orders
GROUP BY DATE(ingested_at)
ORDER BY date DESC;
```

---

## 6. Maintenance

### 6.1 VACUUM (Weekly — Sunday 3AM)

`vacuum_all_tables` job runs automatically. To run manually:

```
Jobs UI → vacuum_all_tables → Run Now (Retention is 168 hours (7 days) — never lower)
```

### 6.2 Monitor Azure Credits

NAT Gateway burns ~₹100–150/day regardless of workload. Check Azure Cost Management daily. Stop streaming jobs when not needed to reduce cluster uptime.

---

## 7. Troubleshooting

### 7.1 Pipeline State Mismatch

**Symptom:** Silver pipeline reads 0 records despite new Bronze data.

```sql
-- Check state vs actual Bronze version
SELECT * FROM streamline.silver.pipeline_state
WHERE pipeline_name = 'bronze_to_silver_orders'
ORDER BY last_run_time DESC LIMIT 5;

DESCRIBE HISTORY streamline.bronze.orders LIMIT 1;

-- Fix: delete bad state row and rerun
DELETE FROM streamline.silver.pipeline_state
WHERE pipeline_name = 'bronze_to_silver_orders'
AND status = 'success'
AND last_run_time = '<bad_run_time>';
```

### 7.2 Bundle State Mismatch

**Symptom:** `databricks bundle destroy` fails with `lineage mismatch in state files`.

```bash
# Delete remote state and redeploy
databricks fs rm -r /Workspace/Users/<user>/.bundle/streamline/dev/terraform/
databricks bundle deploy --target dev
```

### 7.3 Streaming Job — No New Bronze Data

```
1. Check data_generator_and_bronze_continous logs in Jobs UI
2. Verify Kafka credentials in config.py are correct
3. Check Confluent Cloud cluster is active
4. Restart the job
```

### 7.4 DLT Pipeline Failing

```
1. DLT UI → streamline_dlt_pipeline → Event Log
2. Schema mismatch → check bronze schema vs gold_dlt_realtime.py
3. Checkpoint corruption → delete checkpoint and restart:
   abfss://streamline@streamlinedevstorage.dfs.core.windows.net/checkpoints/dlt/
4. Restart from DLT UI
```

### 7.5 High Quarantine Rate

```sql
SELECT rejection_reason, COUNT(*) as count
FROM streamline.silver.orders_quarantine
WHERE DATE(created_at) = current_date()
GROUP BY rejection_reason
ORDER BY count DESC;
```

Fix upstream issue then run `reprocess_quarantine` job.

### 7.6 Dead Letter Queue Growing

```sql
SELECT topic, error, COUNT(*) as count
FROM streamline.bronze.dead_letter
GROUP BY topic, error
ORDER BY count DESC;
```

Fix upstream Kafka message format then run `reprocess_dlq` job with the affected topic.

---

*Last updated: April 2026*
