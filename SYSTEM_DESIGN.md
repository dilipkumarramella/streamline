# Streamline — System Design

> Kafka-Powered Real-Time Analytics Platform for E-Commerce  
> Stack: PySpark · Delta Lake · Databricks · Azure ADLS Gen2 · Unity Catalog · Confluent Kafka · DLT · GitHub Actions · DAB

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Diagram](#2-architecture-diagram)
3. [Data Flow — End to End](#3-data-flow--end-to-end)
4. [Infrastructure](#4-infrastructure)
5. [Key Design Decisions](#5-key-design-decisions)
6. [Data Quality Framework](#6-data-quality-framework)
7. [CI/CD Pipeline](#7-cicd-pipeline)
8. [Repository Structure](#8-repository-structure)
9. [Known Limitations](#9-known-limitations)

---

## 1. Project Overview

Streamline is a production-pattern data engineering project simulating a real e-commerce analytics platform. It ingests raw transactional events (orders, payments, clickstream) from Confluent Kafka, processes them through a medallion architecture (Bronze → Silver → Gold) on Databricks, and serves analytics to PowerBI dashboards.

The project is designed to reflect decisions a data engineer would make at a product company — not just a working pipeline, but a maintainable, observable, and deployable one.

**What it solves:**
- Real-time ingestion from Kafka into Bronze Delta tables (Single Source of Truth)
- Incremental Silver processing with CDF, data quality gates, and quarantine pattern
- Two Gold paths: CDF-incremental batch aggregations for reporting, DLT streaming for operational analytics
- Dead Letter Queue for unparseable Kafka messages with adhoc reprocessing
- Quarantine reprocessing pipeline — rerun bad records through full quality checks
- Full CI/CD with GitHub Actions and Databricks Asset Bundles
- Table-level lineage via Unity Catalog
- Job failure alerting

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CONFLUENT KAFKA (Cloud)                      │
│              orders-topic │ payments-topic │ clickstream-topic      │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ PySpark Structured Streaming (24/7)
                           │ bronze_delta.py + Dead Letter Queue
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER  (ADLS Gen2)                        │
│         streamline.bronze.orders                                    │
│         streamline.bronze.payments          ← Raw, Forever          │
│         streamline.bronze.clickstream         Single Source         │
│         streamline.bronze.dead_letter         of Truth              │
│                                                                     │
│         CDF Enabled on all bronze tables                            │
└──────────┬──────────────────────────────────────┬───────────────────┘
           │                                      │
           │ Batch (daily 2AM)                    │ DLT Continuous (24/7)
           │ CDF Incremental Read                 │ 10-min watermark window
           ▼                                      ▼
┌─────────────────────────┐           ┌──────────────────────────────┐
│     SILVER LAYER        │           │       GOLD REALTIME          │
│                         │           │                              │
│  fact_orders            │           │  live_order_summary          │
│  fact_payments          │           │  live_funnel_snapshot        │
│  fact_events            │           │  live_payment_success_rate   │
│  dim_customer (SCD2)    │           │  (DLT Streaming Pipeline)    │
│  dim_product  (SCD2)    │           └──────────────────────────────┘
│  orders_quarantine      │
│  payments_quarantine    │
│  pipeline_state         │
└──────────┬──────────────┘
           │ Batch (daily 2AM, after Silver)
           │ CDF Incremental Read
           │ Column pruning + affected-row targeting
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       GOLD LAYER (Batch)                            │
│                                                                     │
│  orders_daily_summary     customer_360 (RFM + LTV)                  │
│  funnel_metrics           payment_success_rate                      │
│  data_quality_metrics                                               │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
                    PowerBI Dashboards
```

---

## 3. Data Flow — End to End

### 3.1 Ingestion (Bronze)

`data_generator.py` produces synthetic e-commerce events to three Confluent Kafka topics. `bronze_delta.py` reads from all three topics using PySpark Structured Streaming with schema enforcement. Records land in Bronze Delta tables with an `ingested_at` audit timestamp. Messages that are completely unparseable are routed to `bronze.dead_letter` instead of crashing the stream — separating bad messages (DLQ) from bad values (quarantine).

**Why Bronze is the Single Source of Truth:**
Raw data is never modified or deleted from Bronze. Every downstream pipeline reads from Bronze, not from Kafka directly. Any Silver or Gold pipeline can be fully reprocessed at any time without re-consuming Kafka.

### 3.2 Silver Processing (Incremental Batch)

Each Silver pipeline follows a consistent pattern: read Bronze incrementally via CDF, flatten and clean the data, run schema validation and quality checks, split good and bad records, write good records via merge and bad records to quarantine, update pipeline state, and optimize. The same structure across all five Silver pipelines makes them predictable, debuggable, and easy to test.

### 3.3 Incremental Processing with CDF

Silver pipelines use Delta Change Data Feed to read only new Bronze records since the last successful run. The last processed version is tracked per pipeline in the `pipeline_state` table.

```
First run           → full scan of Bronze
Subsequent runs     → CDF read from last_version + 1
Backfill/Reprocess  → datetime range filter on ingested_at
No new data         → early exit, no processing
```

**Why CDF over Autoloader:** Autoloader is optimised for file-based sources. Since our source is Kafka → Delta, CDF is the correct mechanism — transaction-level reads, exact-once capture regardless of timestamp ordering.

### 3.4 Gold — Batch vs Streaming

| Path | Pipeline | Refresh | Consumer | Purpose |
|---|---|---|---|---|
| Batch | CDF-incremental aggregations | Daily 2AM | PowerBI | Historical analysis, reporting |
| Streaming | DLT Continuous, 10-min watermark | 24/7 | Operational dashboards | Real-time metrics |

Batch Gold runs after all Silver is settled and produces complete daily summaries. DLT Streaming Gold trades completeness for latency, using a 10-minute watermark to handle late-arriving events. Mixing both in one table would require watermarking trade-offs that compromise both SLAs.

### 3.5 Gold Incremental Strategy

Gold batch pipelines do not full-scan Silver on every run. Each uses CDF to identify only the Silver rows that changed since the last run, then recomputes only the affected aggregates. For example, `gold.customer_360` reads changed `fact_orders` rows, extracts affected `customer_id`s, recomputes RFM only for those customers, and merges the results. Column pruning ensures only required columns are read from Silver.

### 3.6 DLQ Reprocessing

`reprocess_dlq.py` is an adhoc maintenance notebook. Given a topic name, it reads `bronze.dead_letter` for that topic, attempts re-parsing with the correct schema, and writes successfully parsed records to the appropriate bronze table via merge. Run from the Jobs UI when upstream message format issues are fixed.

### 3.7 Quarantine Reprocessing

`reprocess_quarantine.py` accepts a `source` parameter (`orders` or `payments`). It reads the quarantine table, runs the same full quality check pipeline as the Silver notebook, writes records that now pass to Silver via merge, and writes still-failing records back to quarantine with updated `rejection_reason`. Idempotent — safe to rerun.

---

## 4. Infrastructure

### 4.1 Azure Resources (`streamline-dev-rg`)

| Resource | Name | Purpose |
|---|---|---|
| ADLS Gen2 | streamlinedevstorage | Raw and processed data storage |
| Databricks | streamline-dev-dws (Premium) | Compute and Unity Catalog |
| Access Connector | Managed Identity | Secure ADLS access from Databricks |
| NAT Gateway | Required | Databricks Premium + Unity Catalog requirement |

**Why ADLS Gen2:** Hierarchical namespace enables atomic directory operations, fine-grained ACLs, and significantly better Delta Lake file listing performance compared to Blob Storage.

**Why Databricks Premium:** Unity Catalog requires Premium. UC provides three-level namespace (`catalog.schema.table`), table-level lineage, fine-grained access control, and cross-workspace data sharing.

### 4.2 Databricks Setup

| Component | Config | Reason |
|---|---|---|
| Cluster | D4ds_v5 Single Node | Cost optimisation — delta cache accelerated |
| Runtime | 17.3 LTS | Long-term support, production stable |
| Photon | OFF | Not needed for current workload size |
| Auto-terminate | 30 mins (interactive) | Credit conservation |
| Job clusters | Separate per job | Cheaper per DBU, auto-terminate on completion |
| Table lineage | Enabled | Unity Catalog table-level lineage tracking |

### 4.3 Unity Catalog Structure

```
streamline (catalog)
├── bronze  (schema) — raw ingested data, retained forever
├── silver  (schema) — cleaned, transformed, quality-gated data
└── gold    (schema) — aggregated, business-ready data
```

**Why external tables:** Data lives in ADLS at a controlled path. If the workspace or catalog metadata is lost, data is not lost — it can be re-registered. Managed tables tie data lifecycle to the catalog, which is a risk in a multi-workspace setup.

### 4.4 Confluent Kafka Setup

| Component | Config | Reason |
|---|---|---|
| Cluster | Basic/Standard | Cost optimisation |
| Region | asia-south1 | Select the region closer to your databricks workspace region |
| Provider | Azure/GCP | Any cloud as per your choice |
| Topics | 3 (orders, payments, clickstream) | Three topics for three tables |

### 4.5 Alerting

Databricks job failure alerts are configured per job. `pipeline_state` records `status = failed` for every pipeline failure, queryable by the orchestrator to block dependent Gold jobs from running on stale Silver data.

---

## 5. Key Design Decisions

### 5.1 Medallion Architecture

**Decision:** Bronze = raw forever. Silver = clean, typed, quality-gated. Gold = aggregated for specific use cases.

**Why:** Each layer has a clear contract. Bronze never changes, so Silver and Gold can always be rebuilt from scratch. Full reprocessability from source is the most important architectural property of the platform.

---

### 5.2 CDF for Incremental Reads Across All Layers

**Decision:** Delta Change Data Feed at every layer — Bronze → Silver and Silver → Gold.

**Why not watermarks:** Watermarks require a monotonically increasing timestamp and can miss late-arriving data. CDF operates at the Delta transaction level, capturing every INSERT/UPDATE/DELETE exactly once regardless of timestamp ordering.

**Why not full scan:** Full scanning Bronze on every Silver run would be O(total data). CDF makes every layer sub-linear in data volume — the benefit compounds across Bronze → Silver → Gold.

---

### 5.3 SCD2 for Customer and Product Dimensions

**Decision:** `dim_customer` and `dim_product` use SCD Type 2 — new row per change, `is_current` flag, `start_date`/`end_date` range.

**Why SCD2 over SCD1:** E-commerce analytics require historical accuracy. Overwriting a customer's city would corrupt all historical revenue attribution. SCD2 preserves history — you can answer "what was this customer's city when this order was placed?" by joining on the order timestamp against the dimension's date range.

**Why not SCD2 for facts:** Facts are immutable business events. An order placed in Mumbai was placed in Mumbai. SCD2 on facts would be meaningless.

**run_number for change detection:** The data generator injects `run_number + 1` into records representing attribute changes. Silver SCD2 pipelines detect changes by comparing incoming `run_number` against the current dimension row — lightweight, avoids full-row hash comparisons.

---

### 5.4 Quarantine Pattern

**Decision:** Bad records are written to quarantine tables with a `rejection_reason` column rather than being dropped.

**Why:** Silently dropping bad records makes data quality invisible and irrecoverable. Quarantine tables make bad data visible, queryable, and reprocessable once the upstream issue is fixed. `gold.data_quality_metrics` aggregates quarantine counts daily so quality trends are trackable over time.

**Why no quarantine for clickstream:** Clickstream is client-side behavioural data. Even if a record is quarantined, there is no source to recover the original event from. Quarantining creates a false impression of recoverability.

---

### 5.5 Dead Letter Queue

**Decision:** Completely unparseable Kafka messages are written to `bronze.dead_letter` instead of crashing the stream.

**Why:** The quarantine pattern handles bad *values*. A message that cannot be parsed into a DataFrame row at all would crash the stream without a DLQ. The DLQ separates two distinct failure modes: bad data (quarantine) vs bad messages (DLQ). Both are observable, recoverable, and reprocessable via `reprocess_dlq.py`.

---

### 5.6 Pipeline State Table

**Decision:** Last successfully processed Bronze version tracked per pipeline in a `pipeline_state` Delta table.

**Why version numbers over timestamps:** Version numbers are monotonically increasing and unambiguous. Timestamps can have clock skew. Version 47 always follows version 46.

**Why per-pipeline state:** Each Silver pipeline is independent. `silver_orders` and `silver_payments` may be at different Bronze versions if one failed. Per-pipeline state allows independent recovery without affecting other pipelines.

---

### 5.7 Static Pools in Delta

**Decision:** Customer (400) and product (150) pools are generated once and stored in Delta. All data generation reads from these pools.

**Why:** Realistic analytics require the same customers and products appearing across multiple orders. Stored pools enable referential integrity across Bronze tables, SCD2 testing (same customer with changed city), and funnel analysis (same customer viewing, adding to cart, and purchasing).

---

### 5.8 Modular Utility Layer

**Decision:** All Delta operations (`delta_helpers.py`) and data quality functions (`data_quality.py`) are centralised utilities imported by every pipeline.

**Why:** Without centralisation, each notebook would implement its own version of `merge_to_delta`, `check_nulls`, etc. — inconsistent behaviour, duplicated bugs, fragmented fixes. Centralised utilities mean fix once, fixed everywhere, and make unit testing straightforward.

---

### 5.9 Modular Jobs (One Job Per Table)

**Decision:** Each Silver and Gold table has its own DAB job. A main orchestrator calls sub-jobs in dependency order.

**Why not monolithic:** A monolithic job fails entirely if one table fails. Modular jobs allow partial success and independent reruns. The orchestrator only runs Gold after all Silver jobs succeed.

---

### 5.10 Backfill via Datetime Widgets

**Decision:** Every Silver pipeline exposes `start_datetime` and `end_datetime` as Databricks job widgets.

**Why:** Backfill is an operational reality. Datetime widgets let operators trigger backfill from the Jobs UI without modifying code. `merge_to_delta` ensures reprocessed records are idempotent — no duplicates regardless of how many times a range is reprocessed.

---

### 5.11 Service Level Objectives

| Pipeline | Freshness SLA | Completeness SLA | Recovery Time |
|----------|---------------|------------------|---------------|
| **Bronze Streaming** | <5min end-to-end | 99.9% (DLQ catches rest) | <15min |
| **Silver Batch** | Daily 2AM | 100% (quarantine → manual) | <2hrs |
| **Gold Batch** | Daily 6AM | 100% | <1hr |
| **Gold DLT** | <15min (10min watermark) | 99% (late data tolerance) | <30min |

**SLA Rationale:**
- **Bronze**: Kafka → Delta must be near-real-time for operational use cases
- **Silver**: Daily batch allows full quality gates + quarantine reprocessing 
- **Gold Batch**: Reporting needs complete daily data, runs after Silver settles
- **Gold DLT**: Trades completeness for latency using watermarking

---

## 6. Data Quality Framework

Every Silver pipeline runs six checks in sequence after cleaning: schema validation (fails pipeline on missing critical columns), null checks, duplicate detection (keeps latest), positive value checks, valid value checks, and future date checks. Each failing check tags the record with a `rejection_reason`. After all checks, `quarantine_records()` splits the DataFrame — clean records go to Silver via merge, tagged records go to the quarantine table.

Bad data is injected by the data generator at a random rate (0–7.5% per run) covering nulls, negative values, future timestamps, invalid statuses, and SCD2-triggering changes — ensuring the quality framework is continuously exercised on every pipeline run.

Unit tests cover all core quality functions with both positive and negative cases. Tests run on every pull request via GitHub Actions before any deployment, using local PySpark with no cluster cost.

---

## 7. CI/CD Pipeline

### 7.1 Branch Strategy

```
main        ← production, protected
  └── develop   ← integration, protected
        └── feature/*  ← active development
```

Pull requests are required for all merges. Direct pushes to `develop` and `main` are blocked.

### 7.2 CI (Pull Request) → CD (Push)

On every PR, GitHub Actions runs unit tests and `databricks bundle validate` against the target environment. Validate catches YAML errors, missing references, and variable resolution failures before they reach the workspace. Tests catch logic errors in utility functions with no cluster cost.

On merge, `databricks bundle deploy` promotes the bundle to the target environment — creating or updating all DAB-managed resources (jobs, DLT pipelines, schemas).

### 7.3 Databricks Asset Bundles (DAB)

All Databricks resources are defined as code in YAML — jobs, DLT pipelines, schemas, permissions. Dev uses `mode: development` with a `name_prefix` preset to avoid resource collision. Prod uses `mode: production` with no prefix. Separate state paths per environment ensure deploy and destroy operations are fully isolated.

**Why DAB over Terraform:** DAB understands Databricks-native resources natively — jobs, DLT pipelines, clusters. It handles state management, permissions, and environment promotion without custom provider configuration.

---

## 8. Repository Structure

```
streamline/
├── .github/
│   └── workflows/
│       ├── ci.yml                      ← validate + unit tests on PR
│       └── cd.yml                      ← deploy on push to develop/main
├── databricks.yml                      ← root config, targets, state paths
├── src/
│   ├── variables.yml                   ← shared variables
│   └── env/
│       ├── dev.yml                     ← dev overrides, permissions, name_prefix
│       └── prod.yml                    ← prod overrides, permissions
├── resources/
│   ├── notebooks/
│   │   ├── utils/
│   │   │   ├── config.py               ← environment config + Kafka credentials
│   │   │   ├── schema_def.py           ← Bronze StructType schemas
│   │   │   ├── delta_helpers.py        ← all Delta read/write/merge/CDF ops
│   │   │   └── data_quality.py         ← all quality check functions
│   │   ├── ingestion/
│   │   │   ├── data_generator.py       ← synthetic data → Kafka producer
│   │   │   └── bronze_delta.py         ← Kafka → Bronze streaming + DLQ
│   │   ├── transformations/
│   │   │   ├── silver_orders.py
│   │   │   ├── silver_payments.py
│   │   │   ├── silver_clickstream.py
│   │   │   ├── silver_customers_dim.py
│   │   │   └── silver_products_dim.py
│   │   ├── gold/
│   │   │   ├── gold_orders_daily.py
│   │   │   ├── gold_customer_360.py
│   │   │   ├── gold_funnel_metrics.py
│   │   │   ├── gold_payment_success.py
│   │   │   └── gold_data_quality_metrics.py
│   │   ├── dlt/
│   │   │   └── gold_dlt_realtime.py
│   │   └── maintenance/
│   │       ├── vacuum.py               ← VACUUM all tables, 7-day retention
│   │       ├── reprocess_dlq.py        ← adhoc DLQ reprocessing by topic
│   │       └── reprocess_quarantine.py ← adhoc quarantine reprocessing
│   ├── jobs/
│   │   ├── data_generator_and_bronze_continous.yml
│   │   ├── reprocess_dlq.yml
│   │   ├── reprocess_quarantine.yml
│   │   ├── streamline_batch_orchestrator_job.yml
│   │   ├── streamline_clickstream_pipeline.yml
│   │   ├── streamline_customers_pipeline.yml
│   │   ├── streamline_orders_silver.yml
│   │   ├── streamline_payments_pipeline.yml
│   │   ├── streamline_products_pipeline.yml
│   │   └── vacuum_all_tables.yml
│   ├── pipelines/
│   │   └── streamline_dlt_pipeline.yml
│   └── schemas/
│       └── streamline_schemas.yml
└── tests/
    └── test_data_quality.py
```

---

## 9. Known Limitations

**DAB schema name prefix bug:**
The `name_prefix` preset in `dev.yml` applies to schema names despite documentation stating otherwise. This is an open Databricks bug — `skip_name_prefix_for_schema` is preview only. Dev schemas are created with a user prefix but pipelines target unprefixed schemas via `config.py`. No impact on prod where `name_prefix` is not used.

**Schema Registry not implemented:**
Confluent Schema Registry is not integrated in this version. Schema enforcement is handled at the Bronze write step via `schema_def.py` StructType schemas. Schema Registry with Avro/Protobuf is planned as a future enhancement.

**Continuous streaming limited by free trial quota:**
Due to Azure free trial credit constraints, the continuous `data_generator_and_bronze_continuous` job and `streamline_dlt_pipeline` cannot be run 24/7 to demonstrate live streaming data in the Gold realtime tables. The streaming architecture is fully implemented and tested — the limitation is infrastructure cost, not code. Streaming Gold tables (`live_order_summary`, `live_funnel_snapshot`, `live_payment_success_rate`) are populated in short demo runs.

**Single-node cluster:**
Dev uses a single D4ds_v5 node for cost reasons. Production would use a multi-node auto-scaling cluster.

---

*Last updated: April 2026*  
*Author: Dilip Kumar Ramella*
