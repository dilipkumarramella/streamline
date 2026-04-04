# Streamline

**Kafka-Powered Real-Time Analytics Platform for E-Commerce**

End-to-end data engineering project built to production standards — not a tutorial. Every architectural decision is deliberate, documented, and defensible. Covers real-time ingestion, medallion architecture, data quality, SCD2 dimensions, CI/CD, and multi-environment deployment on Azure Databricks.

[![Databricks](https://img.shields.io/badge/Databricks-Premium-FF3621?logo=databricks)](https://databricks.com)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Premium-FF3621?logo=databricks)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.x-00ADD8)](https://delta.io)
[![Apache Kafka](https://img.shields.io/badge/Confluent_Kafka-Cloud-231F20?logo=apachekafka)](https://confluent.io)
[![Azure](https://img.shields.io/badge/Azure-ADLS_Gen2-0078D4?logo=microsoftazure)](https://azure.microsoft.com)
[![PySpark](https://img.shields.io/badge/PySpark-3.x-F59E20?logo=apachespark)](https://spark.apache.org)
[![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub_Actions-2088FF?logo=githubactions)](https://github.com/features/actions)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CONFLUENT KAFKA (Cloud)                      │
│              orders-topic │ payments-topic │ clickstream-topic      │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ PySpark Structured Streaming (24/7)
                           │ Schema enforcement · Dead Letter Queue
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE  (ADLS Gen2 · 4 tables)                   │
│         orders · payments · clickstream · dead_letter               │
│         Raw · Forever · CDF Enabled · Single Source of Truth        │
└──────────┬──────────────────────────────────────┬───────────────────┘
           │ CDF Incremental · Batch 2AM           │ DLT Continuous 24/7
           ▼                                       ▼
┌─────────────────────────┐            ┌─────────────────────────────┐
│   SILVER  (8 tables)    │            │   GOLD REALTIME  (3 tables) │
│ fact_orders             │            │   live_order_summary        │
│ fact_payments           │            │   live_funnel_snapshot      │
│ fact_events             │            │   live_payment_success_rate │
│ dim_customer  (SCD2)    │            │   10-min watermark window   │
│ dim_product   (SCD2)    │            └─────────────────────────────┘
│ orders_quarantine       │
│ payments_quarantine     │
│ pipeline_state          │
└──────────┬──────────────┘
           │ CDF · Affected-row targeting · Column pruning · Batch 2AM
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    GOLD BATCH  (5 tables)                           │
│  orders_daily_summary · customer_360 (RFM+LTV) · funnel_metrics     │
│  payment_success_rate · data_quality_metrics                        │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
                    PowerBI Dashboards
```

---

## Why This Project Is Different

Most portfolio projects demonstrate that you *can* use a tool. This one demonstrates *when* to use it, *why* over the alternative, and *what breaks* if you get it wrong.

### Key Decisions

**CDF over Autoloader — at every layer**
Autoloader is for file sources. Kafka → Delta needs CDF — transaction-level reads that capture every change exactly once regardless of timestamp ordering. Applied Bronze→Silver and Silver→Gold so the entire pipeline is O(changed data), not O(total data).

**Quarantine over drop**
Silently dropping bad records is the easiest choice and the most dangerous one. Every bad record gets a `rejection_reason` and lands in a quarantine table — visible, queryable, and reprocessable. `gold.data_quality_metrics` tracks quarantine trends daily.

**Dead Letter Queue — two distinct failure modes**
The quarantine pattern handles bad *values*. A Kafka message that can't be parsed at all crashes the stream without a DLQ. `bronze.dead_letter` separates bad messages (DLQ) from bad values (quarantine) — both observable, both recoverable via dedicated reprocessing jobs.

**SCD2 for dimensions — not facts**
Overwriting a customer's city corrupts all historical revenue attribution. SCD2 preserves history with `start_date`/`end_date` ranges. Change detection uses an injected `run_number` — lightweight, no full-row hash comparisons. Facts are immutable — SCD2 on facts would be meaningless.

**Per-pipeline state tracking**
Each Silver pipeline independently tracks its last processed Bronze version. One failed pipeline never blocks or corrupts another. Recovery is always targeted, never global.

**Gold incremental with affected-row targeting**
Gold batch pipelines don't full-scan Silver. CDF identifies changed rows, pipelines extract only affected IDs, and recompute aggregates for those records only. `customer_360` RFM recomputes only for customers with new orders — not all 400.

---

## Production-Grade Features

| Feature | Details |
|---|---|
| Idempotent pipelines | `merge_to_delta` — safe to rerun any pipeline any number of times |
| CDF incremental | Delta Change Data Feed at every layer — Bronze→Silver→Gold |
| SCD2 dimensions | `dim_customer` and `dim_product` with full history preservation |
| Quarantine pattern | 6 quality checks per Silver pipeline, bad records never dropped |
| Dead Letter Queue | Unparseable Kafka messages → `bronze.dead_letter` |
| DLQ reprocessing | `reprocess_dlq.py` — adhoc reprocessing by topic after fix |
| Quarantine reprocessing | `reprocess_quarantine.py` — reruns bad records through full quality checks |
| Pipeline state tracking | Per-pipeline version tracking drives CDF incremental reads |
| Backfill via widgets | `start_datetime`/`end_datetime` — reprocess any range from Jobs UI |
| Schema evolution | `mergeSchema` enabled — new upstream columns handled automatically |
| Bad data injection | 7 failure scenarios injected at 0–7.5% rate every run |
| Unit tested | 8 pytest tests covering all quality functions — run on every PR |
| Multi-env CI/CD | GitHub Actions → DAB — dev and prod fully separated |
| Modular jobs | One job per table — independent failure, independent recovery |
| Job orchestration | Master orchestrator — Silver parallel → Gold sequential DAG |
| Table-level lineage | Unity Catalog lineage — traceable across Bronze → Silver → Gold |
| DLT watermarking | 10-min watermark for late-arriving event tolerance |
| Affected-row targeting | Gold recomputes only changed records, not full table |
| RFM + LTV | `gold.customer_360` — Recency, Frequency, Monetary segmentation |
| Failure alerting | Job failure email alerts configured per pipeline |
| Weekly VACUUM | Scheduled every Sunday 3AM, 7-day retention |

---

## Tech Stack

| Category | Technology |
|---|---|
| Processing | PySpark, Apache Spark |
| Programming | Python, SQL |
| Storage | Azure ADLS Gen2, Delta Lake |
| Platform | Databricks Premium, Unity Catalog |
| Streaming | Confluent Kafka, DLT, PySpark Structured Streaming |
| Orchestration | Databricks Jobs |
| CI/CD | GitHub Actions, DAB YAML |
| Quality | pytest, custom quality framework |
| Patterns | Medallion, CDF, SCD2, Quarantine, DLQ, RFM |
| Visualisation | PowerBI |

---

## Repository Structure

```
streamline/
├── .github/workflows/
│   ├── ci.yml                          # PR → tests + bundle validate
│   └── cd.yml                          # Merge → bundle deploy
├── databricks.yml                      # DAB root config + state paths
├── src/
│   ├── variables.yml
│   └── env/
│       ├── dev.yml
│       └── prod.yml
├── resources/
│   ├── notebooks/
│   │   ├── utils/
│   │   │   ├── config.py               # Environment config + Kafka credentials
│   │   │   ├── schema_def.py           # Bronze StructType schemas
│   │   │   ├── delta_helpers.py        # Delta read/write/merge/CDF ops
│   │   │   └── data_quality.py         # Quality check functions
│   │   ├── ingestion/
│   │   │   ├── data_generator.py       # Synthetic data → Kafka producer
│   │   │   └── bronze_delta.py         # Kafka → Bronze + DLQ
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
│   │       ├── vacuum.py               # VACUUM all tables, 7-day retention
│   │       ├── reprocess_dlq.py        # Adhoc DLQ reprocessing by topic
│   │       └── reprocess_quarantine.py # Adhoc quarantine reprocessing
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

## Data Model Summary

| Layer | Tables | Grain |
|---|---|---|
| Bronze | 4 | Raw Kafka message |
| Silver | 8 | Fact/Dim (SCD2 for dims) |
| Gold Batch | 5 | Daily aggregations |
| Gold Streaming | 3 | Real-time micro-batch |
| Supporting | 3 | Pools + pipeline state |

Full schema definitions → [DATA_MODEL.md](DATA_MODEL.md)

---

## Demo Videos

| Feature | Video |
|---|---|
| End-to-end pipeline run | _coming soon_ |
| Idempotency — rerun same pipeline | _coming soon_ |
| Backfill — reprocess historical range | _coming soon_ |
| SCD2 — customer location change | _coming soon_ |
| Failure handling — bad data quarantine | _coming soon_ |
| DLQ reprocessing | _coming soon_ |
| CI/CD — PR to prod deployment | _coming soon_ |
| PowerBI dashboard | _coming soon_ |

---

## Documentation

| Doc | Description |
|---|---|
| [SYSTEM_DESIGN.md](SYSTEM_DESIGN.md) | Architecture, data flow, and every key decision with reasoning |
| [DATA_MODEL.md](DATA_MODEL.md) | Full schema for all 23 Delta tables |
| [RUNBOOK.md](RUNBOOK.md) | Operations — deploy, backfill, reprocess, troubleshoot |
| [SETUP_GUIDE.md](SETUP_GUIDE.md) | End-to-end setup from scratch on Azure |

---

## CI/CD

Every PR to `develop` or `main` triggers unit tests via pytest with local PySpark (no cluster cost) and `databricks bundle validate` against the target environment. Every merge triggers `databricks bundle deploy` — promoting all DAB-managed resources to the target environment. Direct push to protected branches is blocked.

---

## Future Scope

- **Confluent Schema Registry** — Avro/Protobuf schema enforcement at the Kafka producer level with version compatibility checks
- **Column-level lineage** — Unity Catalog column-level lineage (currently in preview)
- **Multi-region deployment** — cross-region replication for disaster recovery

---

## Author

**Dilip Kumar Ramella** · Data Engineer · 3 YOE  
Databricks Certified: Data Engineer Associate · DP-300 Azure Database Administrator

[LinkedIn](https://www.linkedin.com/in/dilip-kumar-dataengineer/) · [GitHub](https://github.com/dilipkumarramella)
