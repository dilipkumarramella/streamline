# Databricks notebook source
# ─────────────────────────────────
# IMPORTS
# ─────────────────────────────────
import sys
import os

bundle_root = os.environ.get("BUNDLE_ROOT")
if bundle_root:
    sys.path.append(bundle_root)

from resources.notebooks.utils.config import get_config
from resources.notebooks.utils.delta_helpers import (
    read_data,
    merge_to_delta,
    optimize_table,
    table_exists
)
from resources.notebooks.utils.data_quality import (
    check_nulls,
    check_duplicates,
    check_positive_values,
    check_valid_values,
    check_future_dates,
    quarantine_records
)
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    expr
)
from delta.tables import DeltaTable

# COMMAND ----------

# ─────────────────────────────────
# CONFIGS
# ─────────────────────────────────
env = dbutils.widgets.get("env")
config = get_config(env=env)

source = dbutils.widgets.get("source")

# COMMAND ----------

# ─────────────────────────────────
# PIPELINE CONFIG
# ─────────────────────────────────
def get_pipeline_config(source):
    """
    Get table config based on source.
    Returns quarantine table, silver table,
    merge condition and update set.
    """
    if source == "orders":
        return {
            "quarantine_table": config["silver_orders_quarantine"],
            "silver_table": config["silver_fact_orders"],
            "merge_condition": "target.order_id = source.order_id AND target.product_id = source.product_id",
            "update_set": {
                "order_status": "source.order_status",
                "quantity": "source.quantity",
                "unit_price": "source.unit_price",
                "created_at": "source.created_at"
            }
        }
    elif source == "payments":
        return {
            "quarantine_table": config["silver_payments_quarantine"],
            "silver_table": config["silver_fact_payments"],
            "merge_condition": "target.payment_id = source.payment_id",
            "update_set": {
                "payment_status": "source.payment_status",
                "gateway_response_code": "source.gateway_response_code",
                "retry_count": "source.retry_count",
                "created_at": "source.created_at"
            }
        }

pipeline_config = get_pipeline_config(source)

# COMMAND ----------

# ─────────────────────────────────
# READ QUARANTINE
# ─────────────────────────────────
def read_quarantine():
    """
    Read all records from quarantine table.
    These are records that failed quality
    checks in silver pipeline.
    After fixing root cause at source,
    reprocess them here!
    """
    if not table_exists(spark, pipeline_config["quarantine_table"]):
        raise Exception(
            f"Quarantine table not found: "
            f"{pipeline_config['quarantine_table']}"
        )

    df = read_data(
        spark=spark,
        file_type="delta",
        table_name=pipeline_config["quarantine_table"]
    )

    print(f"Read {df.count()} records from quarantine")
    return df

# COMMAND ----------

# ─────────────────────────────────
# CLEAN DATA
# ─────────────────────────────────
def clean_data(df):
    """
    Clean and standardize DataFrame.
    Same cleaning as silver pipeline!
    """
    if source == "orders":
        if "order_id" in df.columns:
            df = df.withColumn("order_id", lower(trim(col("order_id"))))
        if "customer_id" in df.columns:
            df = df.withColumn("customer_id", lower(trim(col("customer_id"))))
        if "product_id" in df.columns:
            df = df.withColumn("product_id", lower(trim(col("product_id"))))
        if "order_status" in df.columns:
            df = df.withColumn("order_status", lower(trim(col("order_status"))))

        df = df.withColumn("order_timestamp", expr("try_cast(order_timestamp as timestamp)"))
        df = df.withColumn("quantity", expr("try_cast(quantity as int)"))
        df = df.withColumn("unit_price", expr("try_cast(unit_price as double)"))
        df = df.fillna({"order_status": "unknown"})

    elif source == "payments":
        if "payment_id" in df.columns:
            df = df.withColumn("payment_id", lower(trim(col("payment_id"))))
        if "order_id" in df.columns:
            df = df.withColumn("order_id", lower(trim(col("order_id"))))
        if "customer_id" in df.columns:
            df = df.withColumn("customer_id", lower(trim(col("customer_id"))))
        if "payment_status" in df.columns:
            df = df.withColumn("payment_status", lower(trim(col("payment_status"))))
        if "payment_method" in df.columns:
            df = df.withColumn("payment_method", lower(trim(col("payment_method"))))

        df = df.withColumn("payment_timestamp", expr("try_cast(payment_timestamp as timestamp)"))
        df = df.withColumn("amount", expr("try_cast(amount as double)"))
        df = df.withColumn("retry_count", expr("try_cast(retry_count as int)"))
        df = df.fillna({
            "payment_status": "unknown",
            "gateway_response_code": "unknown",
            "retry_count": 0
        })

    return df

# COMMAND ----------

# ─────────────────────────────────
# RUN QUALITY CHECKS
# ─────────────────────────────────
def run_quality_checks(df):
    """
    Read all records from quarantine table.
    These are records that failed quality
    checks in silver pipeline.
    After fixing root cause at source,
    reprocess them here!
    """
    df = df.drop("rejection_reason")

    if source == "orders":
        df = check_nulls(df, [
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "order_timestamp"
        ])
        df = check_duplicates(df, ["order_id", "product_id"], "order_timestamp")
        df = check_positive_values(df, ["quantity", "unit_price"])
        df = check_valid_values(df, {
            "order_status": [
                "delivered", "pending",
                "cancelled", "returned", "unknown"
            ]
        })
        df = check_future_dates(df, ["order_timestamp"])

    elif source == "payments":
        df = check_nulls(df, [
            "payment_id", "order_id", "customer_id",
            "amount", "payment_timestamp"
        ])
        df = check_duplicates(df, ["payment_id"], "payment_timestamp")
        df = check_positive_values(df, ["amount"])
        df = check_valid_values(df, {
            "payment_status": ["success", "failed", "pending", "unknown"],
            "payment_method": ["upi", "card", "cod", "netbanking"]
        })
        df = check_future_dates(df, ["payment_timestamp"])

    return df

# COMMAND ----------

# ─────────────────────────────────
# CLEAN DATA
# ─────────────────────────────────
def clean_data(df):
    """
    Clean and standardize DataFrame.
    Same cleaning as silver pipeline!
    """
    if source == "orders":
        if "order_id" in df.columns:
            df = df.withColumn("order_id", lower(trim(col("order_id"))))
        if "customer_id" in df.columns:
            df = df.withColumn("customer_id", lower(trim(col("customer_id"))))
        if "product_id" in df.columns:
            df = df.withColumn("product_id", lower(trim(col("product_id"))))
        if "order_status" in df.columns:
            df = df.withColumn("order_status", lower(trim(col("order_status"))))

        df = df.withColumn("order_timestamp", expr("try_cast(order_timestamp as timestamp)"))
        df = df.withColumn("quantity", expr("try_cast(quantity as int)"))
        df = df.withColumn("unit_price", expr("try_cast(unit_price as double)"))
        df = df.fillna({"order_status": "unknown"})

    elif source == "payments":
        if "payment_id" in df.columns:
            df = df.withColumn("payment_id", lower(trim(col("payment_id"))))
        if "order_id" in df.columns:
            df = df.withColumn("order_id", lower(trim(col("order_id"))))
        if "customer_id" in df.columns:
            df = df.withColumn("customer_id", lower(trim(col("customer_id"))))
        if "payment_status" in df.columns:
            df = df.withColumn("payment_status", lower(trim(col("payment_status"))))
        if "payment_method" in df.columns:
            df = df.withColumn("payment_method", lower(trim(col("payment_method"))))

        df = df.withColumn("payment_timestamp", expr("try_cast(payment_timestamp as timestamp)"))
        df = df.withColumn("amount", expr("try_cast(amount as double)"))
        df = df.withColumn("retry_count", expr("try_cast(retry_count as int)"))
        df = df.fillna({
            "payment_status": "unknown",
            "gateway_response_code": "unknown",
            "retry_count": 0
        })

    return df

# COMMAND ----------

# ─────────────────────────────────
# RUN QUALITY CHECKS
# ─────────────────────────────────
def run_quality_checks(df):
    """
    Run same quality checks as silver pipeline.
    Good records go to silver.
    Still bad records stay in quarantine
    with updated rejection reason!
    """
    df = df.drop("rejection_reason")

    if source == "orders":
        df = check_nulls(df, [
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "order_timestamp"
        ])
        df = check_duplicates(df, ["order_id", "product_id"], "order_timestamp")
        df = check_positive_values(df, ["quantity", "unit_price"])
        df = check_valid_values(df, {
            "order_status": [
                "delivered", "pending",
                "cancelled", "returned", "unknown"
            ]
        })
        df = check_future_dates(df, ["order_timestamp"])

    elif source == "payments":
        df = check_nulls(df, [
            "payment_id", "order_id", "customer_id",
            "amount", "payment_timestamp"
        ])
        df = check_duplicates(df, ["payment_id"], "payment_timestamp")
        df = check_positive_values(df, ["amount"])
        df = check_valid_values(df, {
            "payment_status": ["success", "failed", "pending", "unknown"],
            "payment_method": ["upi", "card", "cod", "netbanking"]
        })
        df = check_future_dates(df, ["payment_timestamp"])

    return df

# COMMAND ----------

# ─────────────────────────────────
# WRITE RESULTS (UPDATED)
# ─────────────────────────────────
def write_results(df, good_count, bad_count):
    """
    Uses MERGE:
    - Deletes good records from quarantine
    - Updates bad records with new rejection reason
    """

    if good_count == 0 and bad_count == 0:
        print("Nothing to process")
        return

    target = DeltaTable.forName(spark, pipeline_config["quarantine_table"])

    if source == "orders":
        merge_condition = """
            target.order_id = source.order_id
            AND target.product_id = source.product_id
        """
    elif source == "payments":
        merge_condition = """
            target.payment_id = source.payment_id
        """

    target.alias("target") \
        .merge(
            df.alias("source"),
            merge_condition
        ) \
        .whenMatchedDelete(condition="source.rejection_reason IS NULL") \
        .whenMatchedUpdate(set={
            "rejection_reason": "source.rejection_reason"
        }) \
        .execute()

    print(f"Merge complete → cleaned quarantine table")

# COMMAND ----------

# ─────────────────────────────────
# OPTIMIZE
# ─────────────────────────────────
def optimize_tables():
    ""
    Optimize quarantine tables
    after reprocessing.
    Non critical - warns if fails!
    """
    try:
        optimize_table(
            spark=spark,
            table_name=pipeline_config["quarantine_table"]
        )
        print(f"Optimize complete on {pipeline_config['quarantine_table']}")
    except Exception as e:
        print(f"Optimize failed: {str(e)}")
        print("Continuing...")

# COMMAND ----------

# ─────────────────────────────────
# PIPELINE
# ─────────────────────────────────
def run_pipeline():
    """
    Reprocess quarantine records.
    Applies same silver quality checks.
    Good records merged to silver.
    Still bad records updated in quarantine.
    Successfully reprocessed deleted
    from quarantine!
    """
    try:
        print(f"Starting quarantine reprocessing for {source}...")

        df = read_quarantine()

        if df.count() == 0:
            print("No records in quarantine - nothing to reprocess")
            return

        df = clean_data(df)
        print("Cleaning complete")

        df = run_quality_checks(df)
        print("Quality checks complete")

        df.cache()

        good_df, bad_df = quarantine_records(df)
        good_count = good_df.count()
        bad_count = bad_df.count()

        print(f"Records passing checks: {good_count}")
        print(f"Records still failing: {bad_count}")

        # Write to silver (only good)
        if good_count > 0:
            merge_to_delta(
                spark=spark,
                source_df=good_df,
                target_table=pipeline_config["silver_table"],
                merge_condition=pipeline_config["merge_condition"],
                update_set=pipeline_config["update_set"]
            )
            print(f"{good_count} records merged to {pipeline_config['silver_table']}")

        # Clean quarantine using MERGE
        write_results(df, good_count, bad_count)

        optimize_tables()

        print(f"Quarantine reprocessing complete for {source}!")
        print(f"Successfully reprocessed: {good_count}")
        print(f"Still in quarantine: {bad_count}")

    except Exception as e:
        print(f"Reprocessing failed: {str(e)}")
        raise

    finally:
        try:
            df.unpersist()
        except:
            pass

# COMMAND ----------

# ─────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────
run_pipeline()
