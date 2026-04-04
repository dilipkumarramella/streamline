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
    write_data,
    optimize_table,
    table_exists,
    get_last_version,
    get_last_processed_version,
    update_pipeline_state,
    safe_cast,
    column_exists,
    get_table_location,
    scd2_merge,
    read_data
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
    lower,
    lit,
    to_date,
    monotonically_increasing_id,
    row_number
)

from pyspark.sql.window import Window

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
# READ BRONZE ORDERS FOR CUSTOMERS
# ─────────────────────────────────

def read_bronze_customers():
    """
    Read customer data from bronze.orders.
    Uses column pruning to read only
    customer related columns.
    Supports CDF incremental reads
    and backfill/reprocessing mode.

    Returns:
        DataFrame with customer columns only
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
            .select(
                "customer_id",
                "customer_name",
                "city",
                "state",
                "run_number"
            )
        )

    # Normal incremental mode
    else:
        if table_exists(spark, config["silver_dim_customer"]):

            last_version = get_last_processed_version(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="bronze_to_silver_dim_customer"
            )

            if last_version == 0:
                print("No pipeline state found - reading all bronze data")
                df = (
                    spark.read
                    .table(config["bronze_orders"])
                    .select(
                        "customer_id",
                        "customer_name",
                        "city",
                        "state",
                        "run_number"
                    )
                )

            elif last_version >= get_last_version(
                spark, config["bronze_orders"]
            ):
                print("No new versions in bronze - nothing to process")
                return None

            else:
                print(f"Reading CDF from version {last_version + 1}")
                df = (
                    spark.read
                    .format("delta")
                    .option("readChangeFeed", "true")
                    .option("startingVersion", last_version + 1)
                    .table(config["bronze_orders"])
                    .select(
                        "customer_id",
                        "customer_name",
                        "city",
                        "state",
                        "run_number"
                    )
                )

        else:
            print("First run - reading all bronze data")
            df = (
                spark.read
                .table(config["bronze_orders"])
                .select(
                    "customer_id",
                    "customer_name",
                    "city",
                    "state",
                    "run_number"
                )
            )

    cdf_cols = ["_change_type", "_commit_version", "_commit_timestamp"]
    for c in cdf_cols:
        if column_exists(df, c):
            df = df.drop(c)

    return df

# COMMAND ----------

# ─────────────────────────────────
# EXTRACT UNIQUE CUSTOMERS
# ─────────────────────────────────

def extract_unique_customers(df):
    """
    Extract unique customers from bronze.
    Keeps latest record per customer_id
    using window function on run_number.
    Higher run_number = latest record!
    Null customer_ids kept separately
    so all reach quality checks.
    Compare with existing dim_customer
    to find only changed or new customers!

    Args:
        df: Input DataFrame from bronze
            with customer columns and run_number

    Returns:
        DataFrame with changed/new customers only
        Includes null customer_ids for quality checks
    """
    # Separate null and non-null customer_ids
    # Nulls kept separately so ALL reach
    # quality checks not just one!
    null_df = df.filter(col("customer_id").isNull())
    valid_df = df.filter(col("customer_id").isNotNull())

    # Window only on valid customers
    # Keep latest record per customer_id
    window = Window \
        .partitionBy("customer_id") \
        .orderBy(col("run_number").desc())

    unique_valid = valid_df.withColumn(
        "row_num", row_number().over(window)
    ).filter(
        col("row_num") == 1
    ).drop("row_num", "run_number") \
    .select("customer_id", "customer_name", "city", "state")

    # Keep ALL null records
    # Not just one!
    null_df = null_df.drop("run_number") \
        .select("customer_id", "customer_name", "city", "state")

    # Union valid + all nulls
    all_df = unique_valid.union(null_df)

    # Compare with existing dim
    if table_exists(spark, config["silver_dim_customer"]):
        existing = spark.read \
            .table(config["silver_dim_customer"]) \
            .filter(col("is_current") == True) \
            .select(
                "customer_id",
                "customer_name",
                "city",
                "state"
            )

        # left_anti join finds:
        # New customers not in dim
        # Changed customers with
        # different city/state
        changed = all_df.join(
            existing,
            on=["customer_id", "customer_name",
                "city", "state"],
            how="left_anti"
        )

        print(f"Changed/new customers: {changed.count()}")
        return changed

    print(f"New customers: {all_df.count()}")
    return all_df

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
    All string columns lowercased
    for consistent storage.
    Presentation layer handles
    proper casing when needed!
    """
    # Trim and lowercase ALL string columns
    if column_exists(df, "customer_id"):
        df = df.withColumn("customer_id", lower(trim(col("customer_id"))))
    if column_exists(df, "customer_name"):
        df = df.withColumn("customer_name", lower(trim(col("customer_name"))))
    if column_exists(df, "city"):
        df = df.withColumn("city", lower(trim(col("city"))))
    if column_exists(df, "state"):
        df = df.withColumn("state", lower(trim(col("state"))))

    # Safe cast all columns
    df = safe_cast(df, "customer_id", "string")
    df = safe_cast(df, "customer_name", "string")
    df = safe_cast(df, "city", "string")
    df = safe_cast(df, "state", "string")

    # Fill non critical nulls
    df = df.fillna({
        "city": "unknown",
        "state": "unknown",
        "customer_name": "unknown"
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
            "customer_id",
            "customer_name",
            "city",
            "state",
            "created_at"
        ],
        critical_columns=["customer_id"]
    )

    # Check nulls on critical columns
    df = check_nulls(
        df=df,
        columns=["customer_id"]
    )

    # Check duplicates on customer_id
    df = check_duplicates(
        df=df,
        partition_cols=["customer_id"],
        sort_col="created_at"
    )

    return df

# COMMAND ----------

def add_scd2_columns(df):
    """
    Add SCD2 tracking columns.
    Used for first run only!
    """

    df = df.withColumn("customer_sk", monotonically_increasing_id())
    df = df.withColumn("is_current", lit(True))
    df = df.withColumn("start_date", to_date(current_timestamp()))
    df = df.withColumn("end_date", lit(None).cast("date"))

    return df

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO SILVER
# ─────────────────────────────────

def write_to_silver(good_df, good_count):
    """
    Write good records to silver.dim_customer
    using SCD2 merge for history tracking.
    Bad records dropped - no quarantine
    for dimension tables!
    Skips write if count is 0!
    """
    if good_count > 0:
        if table_exists(spark, config["silver_dim_customer"]):

            # Count CURRENT records before merge
            count_before = spark.table(
                config["silver_dim_customer"]
            ).filter(col("is_current") == True).count()

            # SCD2 merge
            scd2_merge(
                spark=spark,
                source_df=good_df,
                target_table=config["silver_dim_customer"],
                natural_key="customer_id",
                tracked_columns=[
                    "customer_name",
                    "city",
                    "state"
                ],
                surrogate_key="customer_sk"
            )

            # Count CURRENT records after merge
            count_after = spark.table(
                config["silver_dim_customer"]
            ).filter(col("is_current") == True).count()

            # Total historical records
            historical_count = spark.table(
                config["silver_dim_customer"]
            ).filter(col("is_current") == False).count()

            new_records = count_after - count_before
            updated_records = good_count - new_records

            print(f"New customers inserted this run: {new_records}")
            print(f"Existing customers updated this run: {updated_records}")
            print(f"Total active customers: {count_after}")
            print(f"Total historical records: {historical_count}")

        else:
            # First run
            good_df = add_scd2_columns(good_df)
            write_data(
                spark=spark,
                df=good_df,
                file_type="delta",
                table_name=config["silver_dim_customer"],
                location=get_table_location(
                    config["silver_path"],
                    config["silver_dim_customer"]
                )
            )
            print(f"First run - {good_count} customers written")

# COMMAND ----------

# ─────────────────────────────────
# OPTIMIZE
# ─────────────────────────────────

def optimize_silver_tables(good_count):
    """
    Run OPTIMIZE on silver dim_customer
    after write.
    Only optimizes if records were written.
    Non critical - warns if fails but
    does not break pipeline!
    """
    if good_count == 0:
        print("No records written - skipping optimize")
        return

    try:
        optimize_table(
            spark=spark,
            table_name=config["silver_dim_customer"],
            zorder_cols=["customer_id"]
        )
        print("Optimize complete on silver.dim_customer")
    except Exception as e:
        print(f"Optimize failed on silver.dim_customer: {str(e)}")
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
    SCD2 for customer dimension!
    No quarantine for dim tables!
    """
    good_count = 0
    bad_count  = 0
    
    try:
        print("Starting silver customers dim pipeline...")

        # Read
        df = read_bronze_customers()

        # Check if no new data
        if df is None:
            print("No new data to process - exiting pipeline")
            return

        print(f"Read {df.count()} records from bronze")

        # Get current bronze version BEFORE processing
        current_version = get_last_version(
            spark, config["bronze_orders"]
        )

        # Extract unique customers
        df = extract_unique_customers(df)

        if df is None or df.count() == 0:
            print("No new or changed customers found")
            print("Updating pipeline state and exiting...")
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="bronze_to_silver_dim_customer",
                last_processed_version=current_version,
                status="success"
            )
            return

        print(f"New or changed customers to process: {df.count()}")

        # Transform
        df = add_audit_columns(df)
        df = clean_data(df)
        print("Transformations complete")

        # Quality checks
        df = run_quality_checks(df)
        print("Quality checks complete")

        # Cache
        df.cache()

        # Filter good records only
        # Bad records dropped - no quarantine!
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
            print(f"Write complete")

        # Update pipeline state on success
        update_pipeline_state(
            spark=spark,
            pipeline_state_table=config["pipeline_state"],
            pipeline_name="bronze_to_silver_dim_customer",
            last_processed_version=current_version,
            status="success",
            good_record_count = good_count,
            bad_record_count = bad_count
        )

        # Optimize
        optimize_silver_tables(good_count)

        print("Silver customers dim pipeline complete")

    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        try:
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="bronze_to_silver_dim_customer",
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

