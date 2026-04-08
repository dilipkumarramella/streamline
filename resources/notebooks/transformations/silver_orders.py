# Databricks notebook source
# ─────────────────────────────────
# IMPORTS
# ─────────────────────────────────
import sys
import os

bundle_root = dbutils.widgets.get("bundle_root")
sys.path.append(bundle_root)

from resources.notebooks.utils.config import get_config
from resources.notebooks.utils.delta_helpers import (
    read_data,
    write_data,
    merge_to_delta,
    optimize_table,
    table_exists,
    get_last_version,
    get_last_processed_version,
    update_pipeline_state,
    safe_cast,
    column_exists,
    get_table_location,
    enable_cdf
)
from resources.notebooks.utils.data_quality import (
    check_nulls,
    check_duplicates,
    check_positive_values,
    check_valid_values,
    check_future_dates,
    quarantine_records,
    validate_schema
)

from pyspark.sql.functions import (
    col,
    explode,
    trim,
    current_timestamp,
    expr,
    lower
)

# COMMAND ----------

# ─────────────────────────────────
# CONFIGS
# ─────────────────────────────────
env = dbutils.widgets.get("env")
config = get_config(env=env)

# Backfill / Reprocessing parameters
dbutils.widgets.text("start_datetime", "")
dbutils.widgets.text("end_datetime", "")

start_datetime = dbutils.widgets.get("start_datetime")
end_datetime = dbutils.widgets.get("end_datetime")

# Validate both provided or both empty
if bool(start_datetime) != bool(end_datetime):
    raise ValueError(
        "Both start_datetime and end_datetime "
        "must be provided together! "
        "Either both empty or both filled."
    )
    
# Validate start < end
if start_datetime and end_datetime:
    if start_datetime >= end_datetime:
        raise ValueError(
            "start_datetime must be less than "
            "end_datetime! "
            f"Got start: {start_datetime} "
            f"end: {end_datetime}"
        )

# COMMAND ----------

# ─────────────────────────────────
# READ BRONZE ORDERS WITH CDF
# ─────────────────────────────────

def read_bronze_orders():
    """
    Read orders from bronze layer.
    Supports two modes:

    Normal mode (no datetime passed):
    -> First run reads all records from bronze
    -> Subsequent runs use CDF incremental read
       to process only new records
    -> If no pipeline state found reads all bronze
       and merge handles duplicates

    Backfill/Reprocessing mode (datetime passed):
    -> Reads bronze filtered by datetime range
    -> Backfill: process missed historical data
    -> Reprocessing: rerun with corrected logic
    -> merge_to_delta ensures no duplicates
       during reprocessing

    Returns:
        DataFrame

    Example:
        # Normal run:
        start_datetime = ""
        end_datetime = ""

        # Backfill or reprocessing:
        start_datetime = "2026-03-13 00:00:00"
        end_datetime = "2026-03-13 23:59:59"

        # Specific hour:
        start_datetime = "2026-03-13 10:00:00"
        end_datetime = "2026-03-13 11:00:00"
    """

    # Check bronze exists first!
    if not table_exists(spark, config["bronze_orders"]):
        raise Exception(
            f"Source table {config['bronze_orders']} not found. "
            f"Upstream ingestion pipeline may have failed. "
            f"Verify bronze layer before retrying."
        )
    
    # Backfill / Reprocessing mode
    if start_datetime and end_datetime:
        print(f"Backfill/Reprocessing mode: {start_datetime} to {end_datetime}")
        df = (
            spark.read
            .table(config["bronze_orders"])
            .where(
                f"ingested_at >= '{start_datetime}' "
                f"AND ingested_at <= '{end_datetime}'"
            )
        )

    # Normal incremental mode
    else:
        if table_exists(spark, config["silver_fact_orders"]):

            # Get last processed version from state
            last_version = get_last_processed_version(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="bronze_to_silver_orders"
            )

            if last_version == 0:
                # No state found
                # Read ALL bronze data
                # merge handles duplicates!
                print("No pipeline state found - reading all bronze data")
                df = read_data(
                    spark=spark,
                    file_type="delta",
                    table_name=config["bronze_orders"]
                )
            
            elif last_version >= get_last_version(spark, config["bronze_orders"]):
                print("No new versions in bronze - nothing to process")
                return None
            
            else:
                # State found
                # CDF incremental read
                print(f"Reading CDF from version {last_version + 1}")
                
                df = read_data(
                    spark=spark,
                    file_type="delta",
                    table_name=config["bronze_orders"],
                    options={
                        "readChangeFeed": "true",
                        "startingVersion": last_version + 1
                    }
                )
                    
        else:
            # First run - read all
            print("First run - reading all bronze data")
            df = read_data(
                spark=spark,
                file_type="delta",
                table_name=config["bronze_orders"]
            )

    cdf_cols = ["_change_type", "_commit_version", "_commit_timestamp"]
    for c in cdf_cols:
        if column_exists(df, c):
            df = df.drop(c)

    return df

# COMMAND ----------

# ─────────────────────────────────
# FLATTEN ORDERS
# ─────────────────────────────────

def flatten_orders(df):
    """
    Explode items array and flatten
    nested item columns.
    One row per order-item!
    """

    # Check items column exists
    if not column_exists(df, "items"):
        print("WARNING: items column missing - skipping explode")
        return df
    
    # Explode items array
    df = df.withColumn("item", explode(col("items")))

    # Flatten known item columns
    df = df.withColumn("product_id", col("item.product_id"))
    df = df.withColumn("product_name", col("item.product_name"))
    df = df.withColumn("category", col("item.category"))
    df = df.withColumn("quantity", col("item.quantity"))
    df = df.withColumn("unit_price", col("item.unit_price"))

    # Drop items and item struct
    df = df.drop("items", "item")

    return df

# COMMAND ----------

# ─────────────────────────────────
# DROP UNNECESSARY COLUMNS
# ─────────────────────────────────

def drop_unnecessary_columns(df):
    """
    Drop columns not needed in fact_orders.
    Customer details go to dim_customer.
    Payment details go to fact_payments.
    """
    df = df.drop(
        "customer_name",   # goes to dim_customer
        "city",            # goes to dim_customer
        "state",           # goes to dim_customer
        "payment_method",  # goes to fact_payments
        "payment_status",  # goes to fact_payments
        "product_name",    # goes to dim_product
        "category",        # goes to dim_product
        "ingested_at",     # bronze only
        "run_number"       # For SCD2
    )

    return df

# COMMAND ----------

# ─────────────────────────────────
# ADD AUDIT COLUMNS
# ─────────────────────────────────

def add_audit_columns(df):
    """
    Add audit columns to DataFrame.
    created_at = when record was
    processed by Silver pipeline!
    """
    df = df.withColumn(
        "created_at",
        current_timestamp()
    )

    return df

# COMMAND ----------

def clean_data(df):
    """
    Clean and standardize DataFrame.
    Trim strings and fill non critical nulls.
    Must run BEFORE quality checks!
    """
    
    # Trim string columns
    if column_exists(df, "order_id"):
        df = df.withColumn("order_id", lower(trim(col("order_id"))))
    if column_exists(df, "customer_id"):
        df = df.withColumn("customer_id", lower(trim(col("customer_id"))))
    if column_exists(df, "product_id"):
        df = df.withColumn("product_id", lower(trim(col("product_id"))))
    if column_exists(df, "order_status"):
        df = df.withColumn("order_status", lower(trim(col("order_status"))))

    # Safe cast
    df = safe_cast(df, "order_id", "string")
    df = safe_cast(df, "customer_id", "string")
    df = safe_cast(df, "product_id", "string")
    df = safe_cast(df, "order_status", "string")
    df = safe_cast(df, "order_timestamp", "timestamp")
    df = safe_cast(df, "quantity", "int")
    df = safe_cast(df, "unit_price", "double")

    # Fill non critical nulls
    df = df.fillna({"order_status": "Unknown"})

    return df

# COMMAND ----------

# ─────────────────────────────────
# RUN QUALITY CHECKS
# ─────────────────────────────────

def run_quality_checks(df):
    """
    Run all quality checks on DataFrame.
    Adds rejection_reason column.
    Must run AFTER clean_data()!
    """

    # Check schema
    validate_schema(
        df=df,
        expected_columns=[
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "order_status",
            "order_timestamp",
            "created_at"
        ],
        critical_columns=["order_id", "customer_id", "product_id"]
    )
    # Check nulls on critical columns
    df = check_nulls(
        df=df,
        columns=["order_id", "customer_id", "product_id",
                 "quantity", "unit_price", "order_timestamp"]
    )

    # Check duplicates on order_id + product_id
    # (one order can have same product twice = duplicate)
    df = check_duplicates(
        df=df,
        partition_cols=["order_id", "product_id"],
        sort_col="order_timestamp"
    )

    # Check positive values
    df = check_positive_values(
        df=df,
        columns=["quantity", "unit_price"]
    )

    # Check valid order status
    df = check_valid_values(
        df=df,
        valid_values_map={
            "order_status": [
                "delivered", "pending",
                "cancelled", "returned",
                "unknown"
            ]
        }
    )

    # Check future dates
    df = check_future_dates(
        df=df,
        columns=["order_timestamp"]
    )

    return df

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO SILVER
# ─────────────────────────────────

def write_to_silver(good_df, bad_df, good_count, bad_count):
    """
    Write good records to silver.fact_orders
    Write bad records to silver.orders_quarantine
    Skips write if count is 0!
    """
    if good_count > 0:
        if table_exists(spark, config["silver_fact_orders"]):
            merge_to_delta(
                spark=spark,
                source_df=good_df,
                target_table=config["silver_fact_orders"],
                merge_condition="target.order_id = source.order_id AND target.product_id = source.product_id",
                update_set={
                    "order_status": "source.order_status",
                    "quantity": "source.quantity",
                    "unit_price": "source.unit_price",
                    "created_at": "source.created_at"
                }
            )
        else:
            write_data(
                spark=spark,
                df=good_df,
                file_type="delta",
                table_name=config["silver_fact_orders"],
                location=get_table_location(
                    config["silver_path"],
                    config["silver_fact_orders"]
                )    
            )
            # Enable CDF only if not already enabled
            enable_cdf(spark, config["silver_fact_orders"])

    if bad_count > 0:
        write_data(
            spark=spark,
            df=bad_df,
            file_type="delta",
            table_name=config["silver_orders_quarantine"],
            location=get_table_location(
                config["silver_path"],
                config["silver_orders_quarantine"]
            )
        )

# COMMAND ----------

# ─────────────────────────────────
# OPTIMIZE
# ─────────────────────────────────

def optimize_silver_tables(good_count, bad_count):
    """
    Run OPTIMIZE on silver tables after write.
    Only optimizes if records were written.
    Non critical - warns if fails but
    does not break pipeline!
    """
    if good_count == 0 and bad_count == 0:
        print("No records written - skipping optimize")
        return

    if good_count > 0:
        try:
            optimize_table(
                spark=spark,
                table_name=config["silver_fact_orders"],
                zorder_cols=["customer_id", "order_timestamp"]
            )
            print("Optimize complete on silver.fact_orders")
        except Exception as e:
            print(f"Optimize failed on silver.fact_orders: {str(e)}")
            print("Continuing pipeline...")

    if bad_count > 0:
        try:
            optimize_table(
                spark=spark,
                table_name=config["silver_orders_quarantine"]
            )
            print("Optimize complete on silver.orders_quarantine")
        except Exception as e:
            print(f"Optimize failed on silver.orders_quarantine: {str(e)}")
            print("Continuing pipeline...")

# COMMAND ----------

# ─────────────────────────────────
# PIPELINE
# ─────────────────────────────────

def run_pipeline():
    """
    Main pipeline function.
    Orchestrates all steps in order.
    Handles errors and pipeline state.
    """
    good_count = 0
    bad_count  = 0
    
    try:
        print("Starting silver orders pipeline...")

        # Read
        df = read_bronze_orders()

        # Check if no new data
        if df is None:
            print("No new data to process - exiting pipeline")
            return
        
        print(f"Read {df.count()} records from bronze")

        # Get current bronze version BEFORE processing
        current_version = get_last_version(
            spark, config["bronze_orders"]
        )

        # Transform
        df = flatten_orders(df)
        df = drop_unnecessary_columns(df)
        df = add_audit_columns(df)
        df = clean_data(df)
        print("Transformations complete")

        # Quality checks
        df = run_quality_checks(df)
        print("Quality checks complete")

        # Cache
        df.cache()

        # Separate good and bad
        good_df, bad_df = quarantine_records(df)
        good_count = good_df.count()
        bad_count = bad_df.count()

        # Write
        if good_count == 0 and bad_count == 0:
            print("No records to write - skipping")
        else:
            write_to_silver(good_df, bad_df, good_count, bad_count)
            print(f"{good_count} records written to {config['silver_fact_orders']}")
            print(f"{bad_count} records written to {config['silver_orders_quarantine']}")

        # Update pipeline state on success
        update_pipeline_state(
            spark=spark,
            pipeline_state_table=config["pipeline_state"],
            pipeline_name="bronze_to_silver_orders",
            last_processed_version=current_version,
            status="success",
            good_record_count = good_count,
            bad_record_count = bad_count
        )

        # Optimize
        optimize_silver_tables(good_count, bad_count)

        print("Silver orders pipeline complete")

    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        try:
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="bronze_to_silver_orders",
                last_processed_version=0,
                status="failed"
            )
        except Exception as state_error:
            print(f"State update failed: {str(state_error)}")
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
