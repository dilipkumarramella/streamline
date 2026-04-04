# Databricks notebook source
# ─────────────────────────────────
# IMPORTS
# ─────────────────────────────────
import sys
import os
import time
from datetime import datetime

bundle_root = os.environ.get("BUNDLE_ROOT")
sys.path.append(bundle_root)

from resources.notebooks.utils.config import get_config
from resources.notebooks.utils.delta_helpers import vacuum_table

# COMMAND ----------

# ─────────────────────────────────
# CONFIG
# ─────────────────────────────────

env = dbutils.widgets.get("env")
config = get_config(env=env)

RETENTION_HOURS = 168  # 7 days

if RETENTION_HOURS < 168:
    raise Exception("Retention must be >= 168 hours")

# COMMAND ----------

# ─────────────────────────────────
# TABLE REGISTRY
# ─────────────────────────────────
LAYER_TABLES = {
    "bronze": [
        "bronze_orders",
        "bronze_payments",
        "bronze_clickstream",
    ],
    "silver": [
        "silver_fact_orders",
        "silver_fact_payments",
        "silver_fact_events",
        "silver_dim_customer",
        "silver_dim_product",
        "silver_orders_quarantine",
        "silver_payments_quarantine",
        "pipeline_state",
    ],
    "gold": [
        "gold_orders_daily",
        "gold_customer_360",
        "gold_funnel_metrics",
        "gold_payment_success",
        "gold_data_quality",
    ],
}

# Flatten all tables
target_tables = [
    config[key]
    for layer in LAYER_TABLES
    for key in LAYER_TABLES[layer]
]

print(f"VACUUM STARTED: {datetime.now()}")
print(f"Tables: {len(target_tables)}")
print(f"Retention: {RETENTION_HOURS} hours")
print("-" * 60)

# COMMAND ----------

# ─────────────────────────────────
# VACUUM
# ─────────────────────────────────
results = []

for table_name in target_tables:
    print(f"\n[{table_name}]")

    if not spark.catalog.tableExists(table_name):
        print("  SKIPPED — table not found")
        results.append((table_name, "skipped"))
        continue

    start = time.time()

    try:
        vacuum_table(
            spark=spark,
            table_name=table_name,
            retention_hours=RETENTION_HOURS,
        )

        elapsed = round(time.time() - start, 1)
        print(f"  OK — {elapsed}s")

        results.append((table_name, "success"))

    except Exception as e:
        print(f"  FAILED — {e}")
        results.append((table_name, "failed"))

# COMMAND ----------

# ─────────────────────────────────
# SUMMARY
# ─────────────────────────────────
success = [t for t, s in results if s == "success"]
failed  = [t for t, s in results if s == "failed"]
skipped = [t for t, s in results if s == "skipped"]

print("\n" + "=" * 60)
print("VACUUM SUMMARY")
print("=" * 60)

print(f"Total   : {len(results)}")
print(f"Success : {len(success)}")
print(f"Failed  : {len(failed)}")
print(f"Skipped : {len(skipped)}")

if failed:
    raise Exception(f"VACUUM failed for: {', '.join(failed)}")

print("=" * 60)
print("VACUUM COMPLETED")
