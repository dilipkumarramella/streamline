# Streamline — Data Model

> All tables are external Delta tables registered in Unity Catalog under `streamline` catalog.  
> Data stored in ADLS Gen2 at `abfss://streamline@streamlinedevstorage.dfs.core.windows.net/`

---

## Table of Contents

1. [Overview](#1-overview)
2. [Bronze Layer](#2-bronze-layer)
3. [Silver Layer](#3-silver-layer)
4. [Gold Layer](#4-gold-layer)
5. [Supporting Tables](#5-supporting-tables)

---

## 1. Overview

```
Layer    Tables    Grain                         Retention    CDF
───────────────────────────────────────────────────────────────────
Bronze   4         Raw event (1 row per message) Forever      Yes
Silver   8         Fact/Dim grain (see below)    Forever      Yes
Gold     8         Aggregated (daily / realtime) Forever      No
Support  3         Pool + state                  Forever      No
───────────────────────────────────────────────────────────────────
Total    23
```

**ADLS paths:**
| Layer | Path |
|---|---|
| Bronze | `.../bronze/` |
| Silver | `.../silver/` |
| Gold | `.../gold/` |
| Checkpoints | `.../checkpoints/` |

---

## 2. Bronze Layer

Raw data ingested from Confluent Kafka via PySpark Structured Streaming. Never modified. Single Source of Truth — all Silver pipelines read from Bronze via CDF.

### 2.1 bronze.orders

Grain: one row per Kafka message (one order per message, items nested as array).

| Column | Type | Nullable | Notes |
|---|---|---|---|
| order_id | STRING | Yes | UUID — business key |
| order_timestamp | TIMESTAMP | Yes | Event time |
| order_status | STRING | Yes | delivered/pending/cancelled/returned |
| customer_id | STRING | Yes | FK to dim_customer |
| customer_name | STRING | Yes | Denormalised from customer |
| city | STRING | Yes | Denormalised from customer |
| state | STRING | Yes | Denormalised from customer |
| items | ARRAY\<STRUCT\> | Yes | Nested — see item schema below |
| payment_method | STRING | Yes | upi/card/cod/netbanking |
| payment_status | STRING | Yes | success/failed/pending |
| run_number | LONG | Yes | SCD2 change detection marker |
| ingested_at | TIMESTAMP | Yes | Added at bronze write time |

**items array schema:**

| Field | Type | Notes |
|---|---|---|
| product_id | STRING | FK to dim_product |
| product_name | STRING | Denormalised |
| category | STRING | Denormalised |
| quantity | INTEGER | — |
| unit_price | DOUBLE | — |
| run_number | LONG | RUN_NUMBER+1 signals category change |

---

### 2.2 bronze.payments

Grain: one row per payment attempt.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| payment_id | STRING | Yes | UUID — business key |
| order_id | STRING | Yes | FK to bronze.orders |
| customer_id | STRING | Yes | FK to dim_customer |
| payment_method | STRING | Yes | upi/card/cod/netbanking |
| payment_status | STRING | Yes | success/failed/pending |
| payment_timestamp | TIMESTAMP | Yes | Event time |
| amount | DOUBLE | Yes | Payment amount |
| transaction_id | STRING | Yes | Gateway transaction ID |
| gateway_response_code | STRING | Yes | 00/01/02/05 |
| retry_count | INTEGER | Yes | 0–3 |
| ingested_at | TIMESTAMP | Yes | Added at bronze write time |

---

### 2.3 bronze.clickstream

Grain: one row per user event.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| event_id | STRING | Yes | UUID — business key |
| session_id | STRING | Yes | Browser session ID |
| customer_id | STRING | Yes | FK to dim_customer |
| event_type | STRING | Yes | view/add_to_cart/checkout/purchase |
| product_id | STRING | Yes | FK to dim_product |
| event_timestamp | TIMESTAMP | Yes | Event time |
| device | STRING | Yes | mobile/desktop/tablet |
| ingested_at | TIMESTAMP | Yes | Added at bronze write time |

---

### 2.4 bronze.dead_letter

Grain: one row per unparseable Kafka message. Stream never crashes — bad messages land here instead.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| raw_message | STRING | Yes | Original Kafka message as string |
| error | STRING | Yes | Parse failure reason |
| topic | STRING | Yes | Source Kafka topic |
| ingested_at | TIMESTAMP | Yes | Added at bronze write time |

---

## 3. Silver Layer

Cleaned, typed, quality-gated data. Bad records quarantined with `rejection_reason`. All dims use SCD2.

### 3.1 silver.fact_orders

Grain: one row per order-item (exploded from bronze.orders items array).  
Source: bronze.orders via CDF.  
Merge key: `order_id + product_id`

| Column | Type | Notes |
|---|---|---|
| order_id | STRING | PK component |
| customer_id | STRING | FK to dim_customer |
| product_id | STRING | FK to dim_product — PK component |
| quantity | INTEGER | — |
| unit_price | DOUBLE | — |
| order_status | STRING | delivered/pending/cancelled/returned |
| order_timestamp | TIMESTAMP | — |
| created_at | TIMESTAMP | Silver processing time |

---

### 3.2 silver.fact_payments

Grain: one row per payment attempt.  
Source: bronze.payments via CDF.  
Merge key: `payment_id`

| Column | Type | Notes |
|---|---|---|
| payment_id | STRING | PK |
| order_id | STRING | FK to fact_orders |
| customer_id | STRING | FK to dim_customer |
| payment_method | STRING | — |
| payment_status | STRING | success/failed/pending |
| payment_timestamp | TIMESTAMP | — |
| amount | DOUBLE | — |
| transaction_id | STRING | — |
| gateway_response_code | STRING | — |
| retry_count | INTEGER | — |
| created_at | TIMESTAMP | Silver processing time |

---

### 3.3 silver.fact_events

Grain: one row per clickstream event.  
Source: bronze.clickstream via CDF.  
Merge key: `event_id`

| Column | Type | Notes |
|---|---|---|
| event_id | STRING | PK |
| session_id | STRING | — |
| customer_id | STRING | FK to dim_customer |
| event_type | STRING | view/add_to_cart/checkout/purchase |
| product_id | STRING | FK to dim_product |
| event_timestamp | TIMESTAMP | — |
| device | STRING | — |
| created_at | TIMESTAMP | Silver processing time |

---

### 3.4 silver.dim_customer *(SCD2)*

Grain: one row per customer per version (new row on city/state change).  
Source: bronze.orders via CDF.  
Natural key: `customer_id` · Surrogate key: `customer_sk`

| Column | Type | Notes |
|---|---|---|
| customer_sk | LONG | Surrogate PK — monotonically increasing |
| customer_id | STRING | Natural business key |
| customer_name | STRING | — |
| city | STRING | Tracked for SCD2 changes |
| state | STRING | Tracked for SCD2 changes |
| is_current | BOOLEAN | True = active record |
| start_date | DATE | Version effective from |
| end_date | DATE | Null if is_current = true |
| created_at | TIMESTAMP | Silver processing time |

**SCD2 change detection:** `run_number + 1` injected by data generator signals a location change. Silver pipeline detects delta between incoming `run_number` and current dim row.

---

### 3.5 silver.dim_product *(SCD2)*

Grain: one row per product per version (new row on category change).  
Source: bronze.orders items array via CDF.  
Natural key: `product_id` · Surrogate key: `product_sk`

| Column | Type | Notes |
|---|---|---|
| product_sk | LONG | Surrogate PK |
| product_id | STRING | Natural business key |
| product_name | STRING | — |
| category | STRING | Tracked for SCD2 changes |
| is_current | BOOLEAN | True = active record |
| start_date | DATE | Version effective from |
| end_date | DATE | Null if is_current = true |
| created_at | TIMESTAMP | Silver processing time |

---

### 3.6 silver.orders_quarantine

Bad records from silver.fact_orders pipeline.  
Append only — never merged.

Same schema as `silver.fact_orders` plus:

| Column | Type | Notes |
|---|---|---|
| rejection_reason | STRING | null_\<col\> / non_positive_\<col\> / invalid_\<col\> / future_date_\<col\> / duplicate_\<cols\> |

---

### 3.7 silver.payments_quarantine

Bad records from silver.fact_payments pipeline.  
Same schema as `silver.fact_payments` + `rejection_reason`.

---

### 3.8 silver.pipeline_state

Tracks last successfully processed Bronze version per pipeline. Drives CDF incremental reads. Append only — full audit history preserved.

| Column | Type | Notes |
|---|---|---|
| pipeline_name | STRING | e.g. bronze_to_silver_orders |
| last_processed_version | LONG | Last Bronze Delta version processed |
| last_run_time | TIMESTAMP | Pipeline run time |
| status | STRING | success / failed |
| good_record_count | LONG | Records written to Silver |
| bad_record_count | LONG | Records sent to quarantine |

---

## 4. Gold Layer

### 4.1 Batch Gold (daily 2AM)

All batch Gold pipelines read Silver via CDF with affected-row targeting and column pruning — no full scans.

#### gold.orders_daily_summary

Grain: one row per summary_date + category + city.

| Column | Type | Notes |
|---|---|---|
| summary_date | DATE | Aggregation date |
| category | STRING | Product category |
| city | STRING | Customer city |
| total_orders | LONG | Distinct order count |
| total_revenue | DOUBLE | Sum of quantity × unit_price |
| avg_order_value | DOUBLE | total_revenue / total_orders |
| created_at | TIMESTAMP | Gold processing time |

---

#### gold.customer_360

Grain: one row per customer (current snapshot).  
Incremental: CDF on fact_orders → extract affected customer_ids → recompute RFM for those customers only.

| Column | Type | Notes |
|---|---|---|
| customer_id | STRING | PK |
| customer_name | STRING | — |
| city | STRING | From current dim_customer |
| state | STRING | From current dim_customer |
| total_orders | LONG | Lifetime order count |
| total_revenue | DOUBLE | Lifetime revenue |
| avg_order_value | DOUBLE | — |
| first_order_date | DATE | — |
| last_order_date | DATE | — |
| recency_days | INTEGER | Days since last order (R) |
| frequency | LONG | Order count (F) |
| monetary_value | DOUBLE | Total revenue (M) |
| rfm_segment | STRING | champions/loyal/at_risk/lost etc. |
| created_at | TIMESTAMP | Gold processing time |

---

#### gold.funnel_metrics

Grain: one row per event_date.

| Column | Type | Notes |
|---|---|---|
| event_date | DATE | — |
| product_views | LONG | view events |
| add_to_cart | LONG | add_to_cart events |
| checkout_started | LONG | checkout events |
| orders_placed | LONG | purchase events |
| conversion_rate | DOUBLE | orders_placed / product_views |
| created_at | TIMESTAMP | Gold processing time |

---

#### gold.payment_success_rate

Grain: one row per payment_date + payment_method.

| Column | Type | Notes |
|---|---|---|
| payment_date | DATE | — |
| payment_method | STRING | upi/card/cod/netbanking |
| total_attempts | LONG | — |
| successful | LONG | status = success |
| failed | LONG | status = failed |
| success_rate | DOUBLE | successful / total_attempts |
| created_at | TIMESTAMP | Gold processing time |

---

#### gold.data_quality_metrics

Grain: one row per pipeline_date + pipeline_name.

| Column | Type | Notes |
|---|---|---|
| pipeline_date | DATE | — |
| pipeline_name | STRING | — |
| total_records_processed | LONG | good + quarantined |
| good_records | LONG | Written to Silver |
| quarantined_records | LONG | Sent to quarantine |
| quality_score | DOUBLE | good / total × 100 |
| created_at | TIMESTAMP | Gold processing time |

---

### 4.2 Streaming Gold (DLT Continuous, 24/7)

Defined as a DLT pipeline in `resources/notebooks/dlt/gold_dlt_realtime.py`.  
Uses 10-minute watermark for late-arriving event tolerance.  
Three streaming tables — exact schemas defined in DLT pipeline.

| Table | Source | Purpose |
|---|---|---|
| gold.realtime_orders | bronze.orders stream | Live order counts and revenue |
| gold.realtime_events | bronze.clickstream stream | Live funnel metrics |
| gold.realtime_payments | bronze.payments stream | Live payment success rates |

---

## 5. Supporting Tables

### 5.1 bronze.customer_pool

Static pool of 400 customers. Created once, never modified.  
Used by data generator to ensure same customers appear across all runs.

| Column | Type |
|---|---|
| customer_id | STRING |
| customer_name | STRING |
| city | STRING |
| state | STRING |

---

### 5.2 bronze.product_pool

Static pool of 150 products. Created once, never modified.

| Column | Type |
|---|---|
| product_id | STRING |
| product_name | STRING |
| category | STRING |

---

*Last updated: April 2026*
