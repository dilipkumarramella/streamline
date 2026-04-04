# Streamline — Data Model

> All tables are external Delta tables registered in Unity Catalog under `streamline` catalog.  
> Data stored in ADLS Gen2 at `abfss://streamline@streamlinedevstorage.dfs.core.windows.net/`

---

## Table of Contents

1. [Overview](#1-overview)
2. [Table Creation Pattern](#2-Table-Creation-Pattern)
3. [Bronze Layer](#3-bronze-layer)
4. [Silver Layer](#4-silver-layer)
5. [Gold Layer](#5-gold-layer)
6. [Supporting Tables](#6-supporting-tables)

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

## 2. Table Creation Pattern

Streamline does not use hand-written DDL to create tables. All Delta tables are created automatically on first pipeline run via the write_data() utility in delta_helpers.py. The function supports three creation modes depending on the arguments passed:

### Mode 1 — External table (used by all Bronze, Silver, Gold tables)
When both table_name and location are passed, write_data() writes the DataFrame to the ADLS path first, then registers it in Unity Catalog using CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION. This is the pattern used for every production table in this project.
```python
write_data(
    spark=spark,
    df=df,
    file_type="delta",
    table_name=config["silver_fact_orders"],        # streamline.silver.fact_orders
    location=get_table_location(
        config["silver_path"],                       # .../silver/
        config["silver_fact_orders"]                 # → .../silver/fact_orders/
    )
)
```
The get_table_location() helper extracts the short table name from the fully qualified UC name and appends it to the layer base path — so streamline.silver.fact_orders becomes abfss://streamline@.../silver/fact_orders/.

### Mode 2 — Managed table
When only table_name is passed with no location, write_data() calls saveAsTable() — Unity Catalog manages both metadata and data lifecycle. Used for supporting tables like pipeline_state where data lifecycle is tied to the catalog.
```python
write_data(
    spark=spark,
    df=df,
    file_type="delta",
    table_name=config["pipeline_state"]
)
```
### Mode 3 — Path-based write
When only location is passed with no table_name, data is written to ADLS without registering in Unity Catalog. Used for intermediate outputs and checkpoint paths, not for any production tables in this project.

#### Common behaviour across all modes:

- mergeSchema=true is always set — schema evolution is handled automatically without manual ALTER TABLE
- Default write mode is append — tables grow incrementally, data is never overwritten
- CDF is enabled after first write via enable_cdf() — called explicitly in each pipeline after the first write_data() for Bronze and Silver tables
- On subsequent runs, Silver and Gold tables use merge_to_delta() or scd2_merge() instead of write_data() — table_exists() is checked before every write to decide between initial creation and incremental merge

### Why no DDL:
Defining schemas in DDL and then separately in schema_def.py StructType would create two sources of truth that can drift. Spark infers and enforces the schema at write time from the DataFrame itself — which is already validated by validate_schema() before any write happens. mergeSchema handles additive changes automatically, and breaking schema changes are caught by validate_schema() raising an exception before the write.

## 3. Bronze Layer

Raw data ingested from Confluent Kafka via PySpark Structured Streaming. Never modified. Single Source of Truth — all Silver pipelines read from Bronze via CDF.

### 3.1 bronze.orders

**Source:** Confluent Kafka `orders` topic → `bronze_delta.py`  
**Read by:** `streamline.silver.fact_orders`, `streamline.silver.dim_customer`, `streamline.silver.dim_product`  
Grain: one row per Kafka message. Items nested as array.

| Column | Type | Notes |
|---|---|---|
| order_id | STRING | UUID — business key |
| order_timestamp | TIMESTAMP | Event time |
| order_status | STRING | delivered/pending/cancelled/returned |
| customer_id | STRING | FK → dim_customer |
| customer_name | STRING | Denormalised from customer |
| city | STRING | Denormalised from customer |
| state | STRING | Denormalised from customer |
| items | ARRAY\<STRUCT\> | Nested — see item schema below |
| payment_method | STRING | upi/card/cod/netbanking |
| payment_status | STRING | success/failed/pending |
| run_number | LONG | SCD2 change detection marker |
| ingested_at | TIMESTAMP | Added at bronze write time |

**items array schema:**

| Field | Type | Notes |
|---|---|---|
| product_id | STRING | FK → dim_product |
| product_name | STRING | Denormalised |
| category | STRING | Denormalised |
| quantity | INTEGER | — |
| unit_price | DOUBLE | — |
| run_number | LONG | RUN_NUMBER+1 signals category change |

---

### 3.2 bronze.payments

**Source:** Confluent Kafka `payments` topic → `bronze_delta.py`  
**Read by:** `streamline.silver.fact_payments`  
Grain: one row per payment attempt.

| Column | Type | Notes |
|---|---|---|
| payment_id | STRING | UUID — business key |
| order_id | STRING | FK → bronze.orders |
| customer_id | STRING | FK → dim_customer |
| payment_method | STRING | upi/card/cod/netbanking |
| payment_status | STRING | success/failed/pending |
| payment_timestamp | TIMESTAMP | Event time |
| amount | DOUBLE | Payment amount |
| transaction_id | STRING | Gateway transaction ID |
| gateway_response_code | STRING | 00/01/02/05 |
| retry_count | INTEGER | 0–3 |
| ingested_at | TIMESTAMP | Added at bronze write time |

---

### 3.3 bronze.clickstream

**Source:** Confluent Kafka `clickstream` topic → `bronze_delta.py`  
**Read by:** `streamline.silver.fact_events`  
Grain: one row per user event.

| Column | Type | Notes |
|---|---|---|
| event_id | STRING | UUID — business key |
| session_id | STRING | Browser session ID |
| customer_id | STRING | FK → dim_customer |
| event_type | STRING | view/add_to_cart/checkout/purchase |
| product_id | STRING | FK → dim_product |
| event_timestamp | TIMESTAMP | Event time |
| device | STRING | mobile/desktop/tablet |
| ingested_at | TIMESTAMP | Added at bronze write time |

---

### 3.4 bronze.dead_letter

**Source:** Unparseable messages from all 3 Kafka topics → `bronze_delta.py`  
**Read by:** `reprocess_dlq.py` (adhoc)  
Grain: one row per unparseable Kafka message. Stream never crashes — bad messages land here instead.

| Column | Type | Notes |
|---|---|---|
| raw_message | STRING | Original Kafka message as string |
| error | STRING | Parse failure reason |
| topic | STRING | Source Kafka topic |
| ingested_at | TIMESTAMP | Added at bronze write time |

---

## 4. Silver Layer

Cleaned, typed, quality-gated data. Bad records quarantined with `rejection_reason`. All dims use SCD2.

### 4.1 silver.fact_orders

**Source:** `streamline.bronze.orders` via CDF  
**Read by:** `streamline.gold.orders_daily_summary`, `streamline.gold.customer_360` 
Grain: one row per order-item (exploded from items array).  
Merge key: `order_id + product_id`

| Column | Type | Notes |
|---|---|---|
| order_id | STRING | PK component |
| customer_id | STRING | FK → dim_customer |
| product_id | STRING | FK → dim_product — PK component |
| quantity | INTEGER | — |
| unit_price | DOUBLE | — |
| order_status | STRING | delivered/pending/cancelled/returned |
| order_timestamp | TIMESTAMP | — |
| created_at | TIMESTAMP | Silver processing time |

---

### 4.2 silver.fact_payments

**Source:** `streamline.bronze.payments` via CDF  
**Read by:** `streamline.gold.payment_success_rate`
Grain: one row per payment attempt.  
Merge key: `payment_id`

| Column | Type | Notes |
|---|---|---|
| payment_id | STRING | PK |
| order_id | STRING | FK → fact_orders |
| customer_id | STRING | FK → dim_customer |
| payment_method | STRING | — |
| payment_status | STRING | success/failed/pending |
| payment_timestamp | TIMESTAMP | — |
| amount | DOUBLE | — |
| transaction_id | STRING | — |
| gateway_response_code | STRING | — |
| retry_count | INTEGER | — |
| created_at | TIMESTAMP | Silver processing time |

---

### 4.3 silver.fact_events

**Source:** `streamline.bronze.clickstream` via CDF  
**Read by:** `streamline.gold.funnel_metrics`
Grain: one row per clickstream event.  
Merge key: `event_id`

| Column | Type | Notes |
|---|---|---|
| event_id | STRING | PK |
| session_id | STRING | — |
| customer_id | STRING | FK → dim_customer |
| event_type | STRING | view/add_to_cart/checkout/purchase |
| product_id | STRING | FK → dim_product |
| event_timestamp | TIMESTAMP | — |
| device | STRING | — |
| created_at | TIMESTAMP | Silver processing time |

---

### 4.4 silver.dim_customer *(SCD2)*

**Source:** `streamline.bronze.orders` via CDF  
**Read by:** `streamline.gold.customer_360`  
Grain: one row per customer per version (new row on city/state change).  
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

### 4.5 silver.dim_product *(SCD2)*

**Source:** `streamline.bronze.orders` items array via CDF  
**Read by:** `streamline.gold.orders_daily_summary`, `streamline.gold.customer_360`  
Grain: one row per product per version (new row on category change).  
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

### 4.6 silver.orders_quarantine

**Source:** `streamline.bronze.orders` via CDF (bad records from silver.fact_orders pipeline)  
**Read by:** `reprocess_quarantine.py` (adhoc)  
Append only — never merged.

Same schema as `silver.fact_orders` plus:

| Column | Type | Notes |
|---|---|---|
| rejection_reason | STRING | null\_\<col\> / non\_positive\_\<col\> / invalid\_\<col\> / future\_date\_\<col\> / duplicate\_\<cols\> |

---

### 4.7 silver.payments_quarantine

**Source:** `streamline.bronze.payments` via CDF (bad records from silver.fact_payments pipeline)  
**Read by:** `reprocess_quarantine.py` (adhoc)  
Same schema as `silver.fact_payments` + `rejection_reason`.

---

### 4.8 silver.pipeline_state

**Source:** Written by every Silver pipeline after each run  
**Read by:** All Silver pipelines (CDF version tracking), `streamline.gold.data_quality_metrics`  
Tracks last successfully processed Bronze version per pipeline. Append only — full audit history preserved.

| Column | Type | Notes |
|---|---|---|
| pipeline_name | STRING | e.g. bronze_to_silver_orders |
| last_processed_version | LONG | Last Bronze Delta version processed |
| last_run_time | TIMESTAMP | Pipeline run time |
| status | STRING | success / failed |
| good_record_count | LONG | Records written to Silver |
| bad_record_count | LONG | Records sent to quarantine |

---

## 5. Gold Layer

### 5.1 Batch Gold (daily 2AM)

All batch Gold pipelines read Silver via CDF with affected-row targeting and column pruning — no full scans.

#### gold.orders_daily_summary

**Source:** `streamline.silver.fact_orders`, `streamline.silver.dim_product` via CDF  
**Read by:** PowerBI dashboard  
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

**Source:** `streamline.silver.fact_orders`, `streamline.silver.dim_customer` via CDF  
**Read by:** PowerBI dashboard  
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

**Source:** `streamline.silver.fact_events` via CDF  
**Read by:** PowerBI dashboard  
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

**Source:** `streamline.silver.fact_payments` via CDF  
**Read by:** PowerBI dashboard  
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

**Source:** `streamline.silver.pipeline_state`  
**Read by:** PowerBI dashboard  
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

### 5.2 Streaming Gold (DLT Continuous, 24/7)

Defined as a DLT pipeline in `gold_dlt_realtime.py`. Uses 10-minute watermark for late-arriving event tolerance.

#### gold.live_funnel_snapshot

**Source:** `streamline.bronze.clickstream` via DLT streaming  
**Read by:** Operational dashboard  
Grain: one row per event_type (rolling real-time count).

| Column | Type | Notes |
|---|---|---|
| event_type | STRING | view/add_to_cart/checkout/purchase |
| event_count | BIGINT | Rolling count within watermark window |
| created_at | TIMESTAMP | DLT processing time |

---

#### gold.live_order_summary

**Source:** `streamline.bronze.orders` via DLT streaming  
**Read by:** Operational dashboard  
Grain: one row per category (rolling real-time summary).

| Column | Type | Notes |
|---|---|---|
| category | STRING | Product category |
| total_orders | BIGINT | Rolling order count |
| delivered_orders | BIGINT | status = delivered |
| total_revenue | DOUBLE | Rolling revenue sum |
| avg_order_value | DOUBLE | total_revenue / total_orders |
| created_at | TIMESTAMP | DLT processing time |

---

#### gold.live_payment_success_rate

**Source:** `streamline.bronze.payments` via DLT streaming  
**Read by:** Operational dashboard  
Grain: one row per payment_method (rolling real-time rates).

| Column | Type | Notes |
|---|---|---|
| payment_method | STRING | upi/card/cod/netbanking |
| total_attempts | BIGINT | Rolling attempt count |
| successful | BIGINT | status = success |
| failed | BIGINT | status = failed |
| pending | BIGINT | status = pending |
| success_rate | DOUBLE | successful / total_attempts |
| created_at | TIMESTAMP | DLT processing time |

---

## 6. Supporting Tables

### 6.1 bronze.customer_pool

**Source:** Generated once by `data_generator.py`  
**Read by:** `data_generator.py` on every run  
Static pool of 400 customers. Created once, never modified.

| Column | Type |
|---|---|
| customer_id | STRING |
| customer_name | STRING |
| city | STRING |
| state | STRING |

---

### 6.2 bronze.product_pool

**Source:** Generated once by `data_generator.py`  
**Read by:** `data_generator.py` on every run  
Static pool of 150 products. Created once, never modified.

| Column | Type |
|---|---|
| product_id | STRING |
| product_name | STRING |
| category | STRING |

---

*Last updated: April 2026*
