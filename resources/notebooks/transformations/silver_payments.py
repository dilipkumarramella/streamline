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
    trim,
    current_timestamp,
    expr,
    lower
)

# COMMAND ----------

# ─────────────────────────────────
# CONFIGS
# ─────────────────────────────────
# env = dbutils.widgets.get("env")
config = get_config(env="dev")

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
# READ BRONZE PAYMENTS WITH CDF
# ─────────────────────────────────

def read_bronze_payments():
    """
    Read payments from bronze layer.
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
    if not table_exists(spark, config["bronze_payments"]):
        raise Exception(
            f"Source table {config['bronze_payments']} not found. "
            f"Upstream ingestion pipeline may have failed. "
            f"Verify bronze layer before retrying."
        )

    # Backfill / Reprocessing mode
    if start_datetime and end_datetime:
        print(f"Backfill/Reprocessing mode: {start_datetime} to {end_datetime}")
        df = (
            spark.read
            .table(config["bronze_payments"])
            .where(
                f"ingested_at >= '{start_datetime}' "
                f"AND ingested_at <= '{end_datetime}'"
            )
        )

    # Normal incremental mode
    else:
        if table_exists(spark, config["bronze_payments"]):

            # Get last processed version from state
            last_version = get_last_processed_version(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="bronze_to_silver_payments"
            )

            if last_version == 0:
                # No state found
                # Read ALL bronze data
                # merge handles duplicates!
                print("No pipeline state found - reading all bronze data")
                df = read_data(
                    spark=spark,
                    file_type="delta",
                    table_name=config["bronze_payments"]
                )

            elif last_version >= get_last_version(spark, config["bronze_payments"]):
                print("No new versions in bronze - nothing to process")
                return None

            else:
                # State found
                # CDF incremental read
                print(f"Reading CDF from version {last_version + 1}")

                df = read_data(
                    spark=spark,
                    file_type="delta",
                    table_name=config["bronze_payments"],
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
                table_name=config["bronze_payments"]
            )

    cdf_cols = ["_change_type", "_commit_version", "_commit_timestamp"]
    for c in cdf_cols:
        if column_exists(df, c):
            df = df.drop(c)

    return df

# COMMAND ----------

def drop_unnecessary_columns(df):
    """
    Drop columns not needed in fact_payments.
    Only ingested_at is dropped as it
    is bronze audit column only.
    """
    df = df.drop(
        "ingested_at"  # bronze only
    )
    return df

# COMMAND ----------

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

# ─────────────────────────────────
# CLEAN DATA
# ─────────────────────────────────

def clean_data(df):
    """
    Clean and standardize DataFrame.
    Trim strings and cast all columns
    to correct types.
    Uses safe_cast to handle missing columns!
    Must run BEFORE quality checks!
    """
    # Trim string columns
    if column_exists(df, "payment_id"):
        df = df.withColumn("payment_id", lower(trim(col("payment_id"))))
    if column_exists(df, "order_id"):
        df = df.withColumn("order_id", lower(trim(col("order_id"))))
    if column_exists(df, "customer_id"):
        df = df.withColumn("customer_id", lower(trim(col("customer_id"))))
    if column_exists(df, "payment_method"):
        df = df.withColumn("payment_method", lower(trim(col("payment_method"))))
    if column_exists(df, "payment_status"):
        df = df.withColumn("payment_status", lower(trim(col("payment_status"))))
    if column_exists(df, "transaction_id"):
        df = df.withColumn("transaction_id", lower(trim(col("transaction_id"))))
    if column_exists(df, "gateway_response_code"):
        df = df.withColumn("gateway_response_code", lower(trim(col("gateway_response_code"))))

    # Safe cast all columns
    df = safe_cast(df, "payment_id", "string")
    df = safe_cast(df, "order_id", "string")
    df = safe_cast(df, "customer_id", "string")
    df = safe_cast(df, "payment_method", "string")
    df = safe_cast(df, "payment_status", "string")
    df = safe_cast(df, "transaction_id", "string")
    df = safe_cast(df, "gateway_response_code", "string")
    df = safe_cast(df, "payment_timestamp", "timestamp")
    df = safe_cast(df, "amount", "double")
    df = safe_cast(df, "retry_count", "int")

    # Fill non critical nulls
    df = df.fillna({
        "payment_status": "Unknown",
        "gateway_response_code": "Unknown",
        "retry_count": 0
    })

    return df

# COMMAND ----------

# ─────────────────────────────────
# RUN QUALITY CHECKS
# ─────────────────────────────────

def run_quality_checks(df):
    """
    Validate schema and run all
    quality checks on DataFrame.
    Adds rejection_reason column.
    Must run AFTER clean_data()!
    """
    # Schema validation first
    validate_schema(
        df=df,
        expected_columns=[
            "payment_id",
            "order_id",
            "customer_id",
            "payment_method",
            "payment_status",
            "payment_timestamp",
            "amount",
            "transaction_id",
            "gateway_response_code",
            "retry_count",
            "created_at"
        ],
        critical_columns=["payment_id", "order_id", "customer_id"]
    )

    # Check nulls on critical columns
    df = check_nulls(
        df=df,
        columns=[
            "payment_id",
            "order_id",
            "customer_id",
            "amount",
            "payment_timestamp"
        ]
    )

    # Check duplicates on payment_id
    df = check_duplicates(
        df=df,
        partition_cols=["payment_id"],
        sort_col="payment_timestamp"
    )

    # Check positive values
    df = check_positive_values(
        df=df,
        columns=["amount"]
    )

    # Check valid payment status
    df = check_valid_values(
        df=df,
        valid_values_map={
            "payment_status": [
                "success",
                "failed",
                "pending",
                "unknown"
            ],
            "payment_method": [
                "upi",
                "card",
                "cod",
                "netbanking"
            ]
        }
    )

    # Check future dates
    df = check_future_dates(
        df=df,
        columns=["payment_timestamp"]
    )

    return df

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO SILVER
# ─────────────────────────────────

def write_to_silver(good_df, bad_df, good_count, bad_count):
    """
    Write good records to silver.fact_payments
    Write bad records to silver.payments_quarantine
    Skips write if count is 0!
    """
    if good_count > 0:
        if table_exists(spark, config["silver_fact_payments"]):
            merge_to_delta(
                spark=spark,
                source_df=good_df,
                target_table=config["silver_fact_payments"],
                merge_condition="target.payment_id = source.payment_id",
                update_set={
                    "payment_status": "source.payment_status",
                    "gateway_response_code": "source.gateway_response_code",
                    "retry_count": "source.retry_count",
                    "created_at": "source.created_at"
                }
            )
        else:
            write_data(
                spark=spark,
                df=good_df,
                file_type="delta",
                table_name=config["silver_fact_payments"],
                location=get_table_location(
                    config["silver_path"],
                    config["silver_fact_payments"]
                )
            )
            # Enable CDF only if not already enabled
            enable_cdf(spark, config["silver_fact_payments"])
            
    if bad_count > 0:
        write_data(
            spark=spark,
            df=bad_df,
            file_type="delta",
            table_name=config["silver_payments_quarantine"],
            location=get_table_location(
                config["silver_path"],
                config["silver_payments_quarantine"]
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
                table_name=config["silver_fact_payments"],
                zorder_cols=["customer_id", "payment_timestamp"]
            )
            print("Optimize complete on silver.fact_payments")
        except Exception as e:
            print(f"Optimize failed on silver.fact_payments: {str(e)}")
            print("Continuing pipeline...")

    if bad_count > 0:
        try:
            optimize_table(
                spark=spark,
                table_name=config["silver_payments_quarantine"]
            )
            print("Optimize complete on silver.payments_quarantine")
        except Exception as e:
            print(f"Optimize failed on silver.payments_quarantine: {str(e)}")
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
        print("Starting silver payments pipeline...")

        # Read
        df = read_bronze_payments()

        # Check if no new data
        if df is None:
            print("No new data to process - exiting pipeline")
            return

        print(f"Read {df.count()} records from bronze")

        # Get current bronze version BEFORE processing
        current_version = get_last_version(
            spark, config["bronze_payments"]
        )

        # Transform
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
            print(f"{good_count} records written to {config['silver_fact_payments']}")
            print(f"{bad_count} records written to {config['silver_payments_quarantine']}")

        # Update pipeline state on success
        update_pipeline_state(
            spark=spark,
            pipeline_state_table=config["pipeline_state"],
            pipeline_name="bronze_to_silver_payments",
            last_processed_version=current_version,
            status="success",
            good_record_count = good_count,
            bad_record_count = bad_count
        )

        # Optimize
        optimize_silver_tables(good_count, bad_count)

        print("Silver payments pipeline complete")

    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        try:
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="bronze_to_silver_payments",
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
