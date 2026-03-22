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
# READ BRONZE CLICKSTREAM WITH CDF
# ─────────────────────────────────

def read_bronze_clickstream():
    """
    Read clickstream from bronze layer.
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
    if not table_exists(spark, config["bronze_clickstream"]):
        raise Exception(
            f"Source table {config['bronze_clickstream']} not found. "
            f"Upstream ingestion pipeline may have failed. "
            f"Verify bronze layer before retrying."
        )

    # Backfill / Reprocessing mode
    if start_datetime and end_datetime:
        print(f"Backfill/Reprocessing mode: {start_datetime} to {end_datetime}")
        df = (
            spark.read
            .table(config["bronze_clickstream"])
            .where(
                f"ingested_at >= '{start_datetime}' "
                f"AND ingested_at <= '{end_datetime}'"
            )
        )

    # Normal incremental mode
    else:
        if table_exists(spark, config["silver_fact_events"]):

            # Get last processed version from state
            last_version = get_last_processed_version(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="bronze_to_silver_clickstream"
            )

            if last_version == 0:
                # No state found
                # Read ALL bronze data
                # merge handles duplicates!
                print("No pipeline state found - reading all bronze data")
                df = read_data(
                    spark=spark,
                    file_type="delta",
                    table_name=config["bronze_clickstream"]
                )

            elif last_version >= get_last_version(spark, config["bronze_clickstream"]):
                print("No new versions in bronze - nothing to process")
                return None

            else:
                # State found
                # CDF incremental read
                print(f"Reading CDF from version {last_version + 1}")

                df = read_data(
                    spark=spark,
                    file_type="delta",
                    table_name=config["bronze_clickstream"],
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
                table_name=config["bronze_clickstream"]
            )

    cdf_cols = ["_change_type", "_commit_version", "_commit_timestamp"]
    for c in cdf_cols:
        if column_exists(df, c):
            df = df.drop(c)

    return df

# COMMAND ----------

# ─────────────────────────────────
# DROP UNNECESSARY COLUMNS
# ─────────────────────────────────

def drop_unnecessary_columns(df):
    """
    Drop columns not needed in fact_events.
    Only ingested_at is dropped as it
    is bronze audit column only.
    """
    df = df.drop(
        "ingested_at"  # bronze only
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
    if column_exists(df, "event_id"):
        df = df.withColumn("event_id", lower(trim(col("event_id"))))
    if column_exists(df, "session_id"):
        df = df.withColumn("session_id", lower(trim(col("session_id"))))
    if column_exists(df, "customer_id"):
        df = df.withColumn("customer_id", lower(trim(col("customer_id"))))
    if column_exists(df, "event_type"):
        df = df.withColumn("event_type", lower(trim(col("event_type"))))
    if column_exists(df, "product_id"):
        df = df.withColumn("product_id", lower(trim(col("product_id"))))
    if column_exists(df, "device"):
        df = df.withColumn("device", lower(trim(col("device"))))

    # Safe cast all columns
    df = safe_cast(df, "event_id", "string")
    df = safe_cast(df, "session_id", "string")
    df = safe_cast(df, "customer_id", "string")
    df = safe_cast(df, "event_type", "string")
    df = safe_cast(df, "product_id", "string")
    df = safe_cast(df, "device", "string")
    df = safe_cast(df, "event_timestamp", "timestamp")

    # Fill non critical nulls
    df = df.fillna({
        "device": "Unknown",
        "session_id": "Unknown"
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
            "event_id",
            "session_id",
            "customer_id",
            "event_type",
            "product_id",
            "event_timestamp",
            "device",
            "created_at"
        ],
        critical_columns=["event_id", "session_id", "customer_id"]
    )

    # Check nulls on critical columns
    df = check_nulls(
        df=df,
        columns=[
            "event_id",
            "customer_id",
            "event_type",
            "product_id",
            "event_timestamp"
        ]
    )

    # Check duplicates on event_id
    df = check_duplicates(
        df=df,
        partition_cols=["event_id"],
        sort_col="event_timestamp"
    )

    # Check valid event type and device
    df = check_valid_values(
        df=df,
        valid_values_map={
            "event_type": [
                "view",
                "add_to_cart",
                "checkout",
                "purchase"
            ],
            "device": [
                "mobile",
                "desktop",
                "tablet",
                "unknown"
            ]
        }
    )

    # Check future dates
    df = check_future_dates(
        df=df,
        columns=["event_timestamp"]
    )

    return df

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO SILVER
# ─────────────────────────────────

def write_to_silver(good_df, good_count):
    """
    Write good records to silver.fact_events
    Bad records are dropped - no quarantine
    for clickstream behavioral data!
    Skips write if count is 0!
    """
    if good_count > 0:
        if table_exists(spark, config["silver_fact_events"]):
            merge_to_delta(
                spark=spark,
                source_df=good_df,
                target_table=config["silver_fact_events"],
                merge_condition="target.event_id = source.event_id",
                update_set={
                    "event_type": "source.event_type",
                    "device": "source.device",
                    "created_at": "source.created_at"
                }
            )
        else:
            write_data(
                spark=spark,
                df=good_df,
                file_type="delta",
                table_name=config["silver_fact_events"],
                location=get_table_location(
                    config["silver_path"],
                    config["silver_fact_events"]
                )
            )
            # Enable CDF only if not already enabled
            enable_cdf(spark, config["silver_fact_events"])

# COMMAND ----------

# ─────────────────────────────────
# OPTIMIZE
# ─────────────────────────────────

def optimize_silver_tables(good_count):
    """
    Run OPTIMIZE on silver tables after write.
    Only optimizes if records were written.
    Non critical - warns if fails but
    does not break pipeline!
    No quarantine table for clickstream!
    """
    if good_count == 0:
        print("No records written - skipping optimize")
        return

    try:
        optimize_table(
            spark=spark,
            table_name=config["silver_fact_events"],
            zorder_cols=["customer_id", "event_timestamp"]
        )
        print("Optimize complete on silver.fact_events")
    except Exception as e:
        print(f"Optimize failed on silver.fact_events: {str(e)}")
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
    No quarantine for clickstream!
    """
    good_count = 0
    bad_count  = 0
    try:
        print("Starting silver clickstream pipeline...")

        # Read
        df = read_bronze_clickstream()

        # Check if no new data
        if df is None:
            print("No new data to process - exiting pipeline")
            return

        print(f"Read {df.count()} records from bronze")

        # Get current bronze version BEFORE processing
        current_version = get_last_version(
            spark, config["bronze_clickstream"]
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

        # Filter good records only
        # No quarantine for clickstream!
        good_df = df.filter(
            col("rejection_reason").isNull()
        ).drop("rejection_reason")
        bad_count = df.filter(
            col("rejection_reason").isNotNull()
        ).count()
        good_count = good_df.count()

        # Write
        if good_count == 0:
            print("No records to write - skipping")
        else:
            write_to_silver(good_df, good_count)
            print(f"{good_count} records written to {config['silver_fact_events']}")

        # Update pipeline state on success
        update_pipeline_state(
            spark=spark,
            pipeline_state_table=config["pipeline_state"],
            pipeline_name="bronze_to_silver_clickstream",
            last_processed_version=current_version,
            status="success",
            good_record_count = good_count,
            bad_record_count = bad_count
        )

        # Optimize
        optimize_silver_tables(good_count)

        print("Silver clickstream pipeline complete")

    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        try:
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="bronze_to_silver_clickstream",
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
