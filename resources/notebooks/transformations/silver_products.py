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
    scd2_merge
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
    row_number,
    explode
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

# ───────────────────────────────────────────────────
# READ BRONZE ITEMS OF ORDERS TABLE FOR PRODUCTS
# ───────────────────────────────────────────────────

def read_bronze_products():
    """
    Read product data from bronze.orders.
    Uses column pruning to read only
    items column then explodes it.
    Supports CDF incremental reads
    and backfill/reprocessing mode.

    Returns:
        DataFrame with product columns only:
        product_id, product_name,
        category, run_number
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
            .select("items")
        )

    # Normal incremental mode
    else:
        if table_exists(spark, config["silver_dim_product"]):

            last_version = get_last_processed_version(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="bronze_to_silver_dim_product"
            )

            if last_version == 0:
                print("No pipeline state found - reading all bronze data")
                df = (
                    spark.read
                    .table(config["bronze_orders"])
                    .select("items")
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
                    .select("items")
                )

        else:
            print("First run - reading all bronze data")
            df = (
                spark.read
                .table(config["bronze_orders"])
                .select("items")
            )

    # Explode items array
    # Extract product columns only
    df = df.withColumn(
        "item", explode(col("items"))
    ).select(
        col("item.product_id"),
        col("item.product_name"),
        col("item.category"),
        col("item.run_number")
    )

    cdf_cols = ["_change_type", "_commit_version", "_commit_timestamp"]
    for c in cdf_cols:
        if column_exists(df, c):
            df = df.drop(c)
            
    return df

# COMMAND ----------

def extract_unique_products(df):
    """
    Extract unique products from bronze.
    Keeps latest record per product_id
    using window function on run_number.
    Higher run_number = latest record!
    Null product_ids kept separately
    so all reach quality checks.
    Compare with existing dim_product
    to find only changed or new products!

    Args:
        df: Input DataFrame from bronze
            with product columns and run_number

    Returns:
        DataFrame with changed/new products only
        Includes null product_ids for quality checks
    """
    
    # Separate null and non-null product_ids
    # Nulls kept separately so ALL reach
    # quality checks not just one!
    null_df = df.filter(col("product_id").isNull())
    valid_df = df.filter(col("product_id").isNotNull())

    # Window only on valid products
    # Keep latest record per product_id
    window = Window \
        .partitionBy("product_id") \
        .orderBy(col("run_number").desc())

    unique_valid = valid_df.withColumn(
        "row_num", row_number().over(window)
    ).filter(
        col("row_num") == 1
    ).drop("row_num", "run_number") \
    .select("product_id", "product_name", "category")

    # Keep ALL null records
    # Not just one!
    null_df = null_df.drop("run_number") \
        .select("product_id", "product_name", "category")

    # Union valid + all nulls
    all_df = unique_valid.union(null_df)

    # Compare with existing dim
    if table_exists(spark, config["silver_dim_product"]):
        existing = spark.read \
            .table(config["silver_dim_product"]) \
            .filter(col("is_current") == True) \
            .select(
                "product_id",
                "product_name",
                "category"
            )

        # left_anti join finds:
        # New products not in dim
        # Changed products with
        # different category/name
        changed = all_df.join(
            existing,
            on=["product_id", "product_name",
                "category"],
            how="left_anti"
        )

        print(f"Changed/new products: {changed.count()}")
        return changed

    print(f"New products: {all_df.count()}")
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

# ─────────────────────────────────
# CLEAN DATA
# ─────────────────────────────────

def clean_data(df):
    """
    Clean and standardize DataFrame.
    All string columns lowercased
    for consistent storage.
    Presentation layer handles
    proper casing when needed!
    """
    # Trim and lowercase ALL string columns
    if column_exists(df, "product_id"):
        df = df.withColumn("product_id", lower(trim(col("product_id"))))
    if column_exists(df, "product_name"):
        df = df.withColumn("product_name", lower(trim(col("product_name"))))
    if column_exists(df, "category"):
        df = df.withColumn("category", lower(trim(col("category"))))

    # Safe cast all columns
    df = safe_cast(df, "product_id", "string")
    df = safe_cast(df, "product_name", "string")
    df = safe_cast(df, "category", "string")

    # Fill non critical nulls
    df = df.fillna({
        "product_name": "unknown",
        "category": "unknown"
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
            "product_id",
            "product_name",
            "category",
            "created_at"
        ],
        critical_columns=["product_id"]
    )

    # Check nulls on critical columns
    df = check_nulls(
        df=df,
        columns=["product_id"]
    )

    # Check duplicates on product_id
    df = check_duplicates(
        df=df,
        partition_cols=["product_id"],
        sort_col="created_at"
    )

    return df

# COMMAND ----------

def add_scd2_columns(df):
    """
    Add SCD2 tracking columns.
    Used for first run only!
    """

    df = df.withColumn("product_sk", monotonically_increasing_id())
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
    Write good records to silver.dim_product
    using SCD2 merge for history tracking.
    Bad records dropped - no quarantine
    for dimension tables!
    Skips write if count is 0!
    """
    if good_count > 0:
        if table_exists(spark, config["silver_dim_product"]):

            # Count CURRENT records before merge
            count_before = spark.table(
                config["silver_dim_product"]
            ).filter(col("is_current") == True).count()

            # SCD2 merge
            scd2_merge(
                spark=spark,
                source_df=good_df,
                target_table=config["silver_dim_product"],
                natural_key="product_id",
                tracked_columns=[
                    "product_name",
                    "category"
                ],
                surrogate_key="product_sk"
            )

            # Count CURRENT records after merge
            count_after = spark.table(
                config["silver_dim_product"]
            ).filter(col("is_current") == True).count()

            # Total historical records
            historical_count = spark.table(
                config["silver_dim_product"]
            ).filter(col("is_current") == False).count()

            new_records = count_after - count_before
            updated_records = good_count - new_records

            print(f"New products inserted this run: {new_records}")
            print(f"Existing products updated this run: {updated_records}")
            print(f"Total active products: {count_after}")
            print(f"Total historical records: {historical_count}")

        else:
            # First run
            good_df = add_scd2_columns(good_df)
            write_data(
                spark=spark,
                df=good_df,
                file_type="delta",
                table_name=config["silver_dim_product"],
                location=get_table_location(
                    config["silver_path"],
                    config["silver_dim_product"]
                )
            )
            print(f"First run - {good_count} products written")

# COMMAND ----------

# ─────────────────────────────────
# OPTIMIZE
# ─────────────────────────────────

def optimize_silver_tables(good_count):
    """
    Run OPTIMIZE on silver dim_product
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
            table_name=config["silver_dim_product"],
            zorder_cols=["product_id"]
        )
        print("Optimize complete on silver.dim_product")
    except Exception as e:
        print(f"Optimize failed on silver.dim_product: {str(e)}")
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
    SCD2 for product dimension!
    No quarantine for dim tables!
    """
    good_count = 0
    bad_count  = 0
    try:
        print("Starting silver products dim pipeline...")

        # Read
        df = read_bronze_products()

        # Check if no new data
        if df is None:
            print("No new data to process - exiting pipeline")
            return

        print(f"Read {df.count()} records from bronze")

        # Get current bronze version BEFORE processing
        current_version = get_last_version(
            spark, config["bronze_orders"]
        )

        # Extract unique products
        df = extract_unique_products(df)

        if df is None or df.count() == 0:
            print("No new or changed products found")
            print("Updating pipeline state and exiting...")
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="bronze_to_silver_dim_product",
                last_processed_version=current_version,
                status="success"
            )
            return

        print(f"New or changed products to process: {df.count()}")

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
            print("Write complete")

        # Update pipeline state on success
        update_pipeline_state(
            spark=spark,
            pipeline_state_table=config["pipeline_state"],
            pipeline_name="bronze_to_silver_dim_product",
            last_processed_version=current_version,
            status="success",
            good_record_count = good_count,
            bad_record_count = bad_count
        )

        # Optimize
        optimize_silver_tables(good_count)

        print("Silver products dim pipeline complete")

    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        try:
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="bronze_to_silver_dim_product",
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
