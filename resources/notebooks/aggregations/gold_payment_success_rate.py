# Databricks notebook source
# ─────────────────────────────────
# IMPORTS
# ─────────────────────────────────
import sys
 
bundle_root = dbutils.widgets.get("bundle_root")
sys.path.append(bundle_root)
 
from resources.notebooks.utils.config import get_config
from resources.notebooks.utils.delta_helpers import (
    read_data,
    write_data,
    optimize_table,
    table_exists,
    get_last_version,
    get_last_processed_version,
    update_pipeline_state,
    get_table_location
)
 
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    when,
    lit,
    current_timestamp,
    round as spark_round
)

# COMMAND ----------

# ─────────────────────────────────
# CONFIGS
# ─────────────────────────────────
env = dbutils.widgets.get("env")
config = get_config(env=env)
 
# Backfill / Reprocessing parameters
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
 
start_date = dbutils.widgets.get("start_date")
end_date   = dbutils.widgets.get("end_date")
 
# Validate both provided or both empty
if bool(start_date) != bool(end_date):
    raise ValueError(
        "Both start_date and end_date "
        "must be provided together! "
        "Either both empty or both filled."
    )
 
# Validate start < end
if start_date and end_date:
    if start_date >= end_date:
        raise ValueError(
            "start_date must be less than end_date! "
            f"Got start: {start_date} end: {end_date}"
        )

# COMMAND ----------

def validate_dependencies():
    if not table_exists(spark, config["silver_fact_payments"]):
        raise Exception(
            f"Dependency check failed. "
            f"Missing table: {config['silver_fact_payments']}. "
            f"Ensure upstream pipelines completed successfully."
        )
    print("Dependency check passed")

# COMMAND ----------

# ─────────────────────────────────
# READ SILVER FACT PAYMENTS
# ─────────────────────────────────
 
def read_silver_payments():
    """
    Read fact_payments from silver layer.
    Supports two modes:
 
    Normal mode (no dates passed):
    -> CDF on silver fact_payments to detect
       which payment_dates have new/changed records
    -> Only those dates recomputed in gold
 
    Backfill/Reprocessing mode (dates passed):
    -> Reads silver filtered by payment date range
    -> Recomputes gold for those dates only
 
    Returns:
        DataFrame or None if nothing to process
    """
    # Backfill / Reprocessing mode
    if start_date and end_date:
        print(f"Backfill/Reprocessing mode: {start_date} to {end_date}")
        df = read_data(
            spark=spark,
            file_type="delta",
            table_name=config["silver_fact_payments"]
        ).filter(
            (to_date(col("payment_timestamp")) >= start_date) &
            (to_date(col("payment_timestamp")) <= end_date)
        )
        return df
 
    # Normal incremental mode
    last_version = get_last_processed_version(
        spark=spark,
        pipeline_state_table=config["pipeline_state"],
        pipeline_name="silver_to_gold_payment_success_rate"
    )
 
    current_silver_version = get_last_version(
        spark, config["silver_fact_payments"]
    )
 
    if last_version == 0:
        print("No pipeline state found - reading all silver data")
        return read_data(
            spark=spark,
            file_type="delta",
            table_name=config["silver_fact_payments"]
        )
 
    if last_version >= current_silver_version:
        print("No new versions in silver - nothing to process")
        return None
 
    # CDF to get affected payment dates
    print(f"Reading CDF from silver version {last_version + 1}")
    cdf_df = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["silver_fact_payments"],
        options={
            "readChangeFeed": "true",
            "startingVersion": last_version + 1
        }
    )
 
    # Get distinct affected dates from CDF
    affected_dates = cdf_df.select(
        to_date(col("payment_timestamp")).alias("payment_date")
    ).distinct()
 
    affected_count = affected_dates.count()
    if affected_count == 0:
        print("CDF returned no affected dates - nothing to process")
        return None
 
    print(f"Affected payment dates to recompute: {affected_count}")
 
    # Read ALL silver payments for affected dates
    # Full day needed for correct aggregation
    df = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["silver_fact_payments"]
    ).join(
        affected_dates,
        to_date(col("payment_timestamp")) == col("payment_date"),
        how="inner"
    ).drop("payment_date")
 
    return df

# COMMAND ----------

# ─────────────────────────────────
# COMPUTE PAYMENT SUCCESS RATE
# ─────────────────────────────────
 
def compute_payment_success_rate(df):
    """
    Aggregate fact_payments to daily grain
    by payment_method.
    Grain: (payment_date, payment_method)
 
    Metrics:
    - total_attempts:  all payment records
    - successful:      payment_status = 'success'
    - failed:          payment_status = 'failed'
    - success_rate:    successful / total_attempts
                       0.0 if total_attempts = 0
 
    Pending payments excluded from success/failed counts
    but included in total_attempts - they are real
    attempts that havent resolved yet.
 
    success_rate rounded to 4 decimal places
    for percentage precision in dashboards.
 
    Args:
        df: silver fact_payments DataFrame
 
    Returns:
        Aggregated gold DataFrame
    """
    gold_df = df.withColumn(
        "payment_date", to_date(col("payment_timestamp"))
    ).groupBy(
        "payment_date",
        "payment_method"
    ).agg(
        count("payment_id").alias("total_attempts"),
        count(
            when(col("payment_status") == "success",
                 col("payment_id"))
        ).alias("successful"),
        count(
            when(col("payment_status") == "failed",
                 col("payment_id"))
        ).alias("failed")
    ).withColumn(
        "success_rate",
        when(
            col("total_attempts") == 0, lit(0.0)
        ).otherwise(
            spark_round(
                col("successful") / col("total_attempts"), 4
            )
        )
    ).withColumn(
        "created_at", current_timestamp()
    ).select(
        "payment_date",
        "payment_method",
        "total_attempts",
        "successful",
        "failed",
        "success_rate",
        "created_at"
    )
 
    return gold_df

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO GOLD
# ─────────────────────────────────
 
def write_to_gold(gold_df, row_count):
    """
    Write payment metrics to gold.payment_success_rate.
    Strategy: dynamic partition overwrite by payment_date.
    Each affected payment_date fully recomputed
    and overwritten - safe to rerun!
 
    Args:
        gold_df: Aggregated gold DataFrame
        row_count: Skip write if 0
    """
    if row_count == 0:
        print("No rows to write - skipping")
        return
 
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode", "dynamic"
    )
 
    if not table_exists(spark, config["gold_payment_success"]):
        print(f"First run - creating {config['gold_payment_success']}")
        write_data(
            spark=spark,
            df=gold_df,
            file_type="delta",
            mode="overwrite",
            table_name=config["gold_payment_success"],
            location=get_table_location(
                config["gold_path"],
                config["gold_payment_success"]
                )
        )
    else:
        write_data(
            spark=spark,
            df=gold_df,
            file_type="delta",
            mode="overwrite",
            table_name=config["gold_payment_success"],
            options={"partitionOverwriteMode": "dynamic"}
        )
 
    print(f"{row_count} rows written to {config['gold_payment_success']}")

# COMMAND ----------

# ─────────────────────────────────
# OPTIMIZE
# ─────────────────────────────────
 
def optimize_gold_table(row_count):
    """
    Run OPTIMIZE on gold payment_success_rate.
    ZORDER by payment_date + payment_method for
    fast filtering in payment health dashboards.
    Non critical - warns if fails but
    does not break pipeline!
 
    Args:
        row_count: Skip optimize if nothing written
    """
    if row_count == 0:
        print("No records written - skipping optimize")
        return
 
    try:
        optimize_table(
            spark=spark,
            table_name=config["gold_payment_success"],
            zorder_cols=["payment_date", "payment_method"]
        )
        print(f"Optimize complete on {config['gold_payment_success']}")
    except Exception as e:
        print(f"Optimize failed: {str(e)}")
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
     
    Flow:
    1. Valedates if all upstream tables and dependencies are met
    2. Read silver fact_payments (CDF incremental)
    3. Aggregate to (payment_date, payment_method) grain
    4. Compute success/failed counts and success_rate
    5. Overwrite affected date partitions in gold
    6. Update pipeline state
    7. Optimize
    """
    gold_df = None
 
    try:
        print("Starting gold payment success rate pipeline...")

        # Validate dependencies
        validate_dependencies()

        # Read silver
        df = read_silver_payments()
 
        if df is None:
            print("No new data to process - exiting pipeline")
            return
 
        record_count = df.count()
        print(f"Read {record_count} records from silver fact_payments")
 
        if record_count == 0:
            print("Zero records after read - exiting pipeline")
            return
 
        # Get current silver version BEFORE processing
        current_version = get_last_version(
            spark, config["silver_fact_payments"]
        )
 
        # Aggregate
        gold_df = compute_payment_success_rate(df)
        print("Payment success rate aggregation complete")
 
        # Cache before count
        gold_df.cache()
        row_count = gold_df.count()
        print(f"Gold rows to write: {row_count}")
 
        # Write
        write_to_gold(gold_df, row_count)
 
        # Update pipeline state on success
        update_pipeline_state(
            spark=spark,
            pipeline_state_table=config["pipeline_state"],
            pipeline_name="silver_to_gold_payment_success_rate",
            last_processed_version=current_version,
            status="success"
        )
 
        # Optimize
        optimize_gold_table(row_count)
 
        print("Gold payment success rate pipeline complete")
 
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        try:
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="silver_to_gold_payment_success_rate",
                last_processed_version=0,
                status="failed"
            )
        except Exception as state_error:
            print(f"State update failed: {str(state_error)}")
        raise
 
    finally:
        try:
            gold_df.unpersist()
        except:
            pass

# COMMAND ----------

# ─────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────
run_pipeline()
