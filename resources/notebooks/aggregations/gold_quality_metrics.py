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
    sum as spark_sum,
    current_timestamp,
    round as spark_round,
    when,
    lit
)

# COMMAND ----------

# ─────────────────────────────────
# CONFIGS
# ─────────────────────────────────
env = dbutils.widgets.get("env")
config = get_config(env=env)
 
PIPELINE_NAME = "silver_to_gold_data_quality"
 
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
    if not table_exists(spark, config["pipeline_state"]):
        raise Exception(
            f"Dependency check failed. "
            f"Missing table: {config['pipeline_state']}. "
            f"Ensure upstream pipelines completed successfully."
        )
    print("Dependency check passed")

# COMMAND ----------

# ─────────────────────────────────
# SILVER PIPELINE NAMES
# ─────────────────────────────────

SILVER_PIPELINES = [
    "bronze_to_silver_orders",
    "bronze_to_silver_payments",
    "bronze_to_silver_clickstream",
    "bronze_to_silver_dim_customer",
    "bronze_to_silver_dim_product"
]

# COMMAND ----------

# ─────────────────────────────────
# READ PIPELINE STATE
# ─────────────────────────────────
 
def read_pipeline_state():
    """
    Read pipeline_state table to get
    good and bad record counts per pipeline per run.
 
    pipeline_state is the single source of truth
    for record counts - silver notebooks write
    good_record_count and bad_record_count at
    the end of every successful run.
 
    No table scans on fact/quarantine tables needed -
    counts already computed by silver pipelines
    and stored here. Lightweight read.
 
    Supports two modes:
 
    Normal mode (no dates passed):
    -> Uses CDF on pipeline_state since last
       processed version
    -> Only recomputes gold for newly added
       pipeline_state rows
 
    Backfill mode (dates passed):
    -> Reads pipeline_state filtered by
       last_run_time date range
    -> Recomputes gold for those dates
 
    Returns:
        DataFrame of pipeline_state rows
        Returns None if nothing to process
    """
    # Backfill / Reprocessing mode
    if start_date and end_date:
        print(f"Backfill/Reprocessing mode: {start_date} to {end_date}")
        df = read_data(
            spark=spark,
            file_type="delta",
            table_name=config["pipeline_state"]
        ).filter(
            (to_date(col("last_run_time")) >= start_date) &
            (to_date(col("last_run_time")) <= end_date) &
            col("pipeline_name").isin(SILVER_PIPELINES)
        )
        return df
 
    # Normal incremental mode
    last_version = get_last_processed_version(
        spark=spark,
        pipeline_state_table=config["pipeline_state"],
        pipeline_name=PIPELINE_NAME
    )
 
    current_state_version = get_last_version(
        spark, config["pipeline_state"]
    )
 
    if last_version == 0:
        print("No pipeline state found - reading all pipeline_state data")
        df = read_data(
            spark=spark,
            file_type="delta",
            table_name=config["pipeline_state"]
        ).filter(
            col("pipeline_name").isin(SILVER_PIPELINES)
        )
        return df
 
    if last_version >= current_state_version:
        print("No new versions in pipeline_state - nothing to process")
        return None
 
    # CDF to get newly added pipeline_state rows
    print(f"Reading CDF from pipeline_state version {last_version + 1}")
    cdf_df = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["pipeline_state"],
        options={
            "readChangeFeed": "true",
            "startingVersion": last_version + 1
        }
    ).filter(
        col("pipeline_name").isin(SILVER_PIPELINES)
    )
 
    # Get distinct affected run dates from CDF
    affected_dates = cdf_df.select(
        to_date(col("last_run_time")).alias("run_date")
    ).distinct()
 
    affected_count = affected_dates.count()
    if affected_count == 0:
        print("No affected dates found - nothing to process")
        return None
 
    print(f"Affected run dates to recompute: {affected_count}")
 
    # Read ALL pipeline_state rows for affected dates
    # A date may have multiple pipeline runs -
    # we need all of them for correct aggregation
    df = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["pipeline_state"]
    ).filter(
        col("pipeline_name").isin(SILVER_PIPELINES)
    ).join(
        affected_dates,
        to_date(col("last_run_time")) == col("run_date"),
        how="inner"
    ).drop("run_date")
 
    return df

# COMMAND ----------

# ─────────────────────────────────
# COMPUTE DATA QUALITY METRICS
# ─────────────────────────────────
 
def compute_quality_metrics(df):
    """
    Aggregate pipeline_state to compute
    quality metrics per pipeline per run date.
 
    Grain: (pipeline_date, pipeline_name)
 
    Why aggregate by date + pipeline_name?
    pipeline_state appends one row per run.
    On reprocessing/backfill a pipeline may
    have multiple rows for same date.
    We sum them to get total counts for that day.
 
    Metrics:
    - total_records_processed: good + bad
    - good_records:            written to silver
    - quarantined_records:     rejected by quality checks
                               named quarantined for
                               business readability even
                               though not all pipelines
                               have quarantine tables
    - quality_score:           good / total
                               null if total = 0
                               (pipeline ran but no data)
 
    Only successful runs counted -
    failed runs have good=0 bad=0 and would
    skew quality_score to 0 incorrectly.
 
    Args:
        df: pipeline_state DataFrame
 
    Returns:
        Aggregated gold data quality DataFrame
    """
    gold_df = df.filter(
        col("status") == "success"
    ).withColumn(
        "pipeline_date", to_date(col("last_run_time"))
    ).groupBy(
        "pipeline_date",
        "pipeline_name"
    ).agg(
        spark_sum("good_record_count").alias("good_records"),
        spark_sum("bad_record_count").alias("quarantined_records")
    ).withColumn(
        "total_records_processed",
        col("good_records") + col("quarantined_records")
    ).withColumn(
        "quality_score",
        when(
            col("total_records_processed") == 0, lit(None).cast("double")
        ).otherwise(
            spark_round(
                col("good_records") / col("total_records_processed"), 4
            )
        )
    ).withColumn(
        "created_at", current_timestamp()
    ).select(
        "pipeline_date",
        "pipeline_name",
        "total_records_processed",
        "good_records",
        "quarantined_records",
        "quality_score",
        "created_at"
    )
 
    return gold_df

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO GOLD
# ─────────────────────────────────
 
def write_to_gold(gold_df, row_count):
    """
    Write quality metrics to gold.data_quality_metrics.
    Strategy: dynamic partition overwrite by pipeline_date.
    Each affected pipeline_date fully recomputed
    and overwritten - safe to rerun!
 
    Args:
        gold_df: Aggregated quality metrics DataFrame
        row_count: Skip write if 0
    """
    if row_count == 0:
        print("No rows to write - skipping")
        return
 
    gold_location = get_table_location(
        config["gold_path"],
        config["gold_data_quality"]
    )
 
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode", "dynamic"
    )
 
    if not table_exists(spark, config["gold_data_quality"]):
        print(f"First run - creating {config['gold_data_quality']}")
        write_data(
            spark=spark,
            df=gold_df,
            file_type="delta",
            mode="overwrite",
            table_name=config["gold_data_quality"],
            location=gold_location,
            partition_cols=["pipeline_date"]
        )
    else:
        write_data(
            spark=spark,
            df=gold_df,
            file_type="delta",
            mode="overwrite",
            table_name=config["gold_data_quality"],
            options={"partitionOverwriteMode": "dynamic"}
        )
 
    print(f"{row_count} rows written to {config['gold_data_quality']}")

# COMMAND ----------

# ─────────────────────────────────
# OPTIMIZE
# ─────────────────────────────────
 
def optimize_gold_table(row_count):
    """
    Run OPTIMIZE on gold data_quality_metrics.
    ZORDER by pipeline_date + pipeline_name for
    fast filtering in observability dashboards.
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
            table_name=config["gold_data_quality"],
            zorder_cols=["pipeline_name"]
        )
        print(f"Optimize complete on {config['gold_data_quality']}")
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
    2. Read pipeline_state (CDF incremental)
       - good_record_count + bad_record_count
         already computed by silver notebooks
       - No table scans on fact/quarantine tables
    3. Filter to successful silver pipeline runs only
    4. Aggregate to (pipeline_date, pipeline_name) grain
    5. Compute quality_score
    6. Overwrite affected date partitions in gold
    7. Update pipeline state
    8. Optimize
 
    Note: This pipeline tracks its own version in
    pipeline_state under PIPELINE_NAME so it knows
    which pipeline_state rows it has already processed.
    """
    gold_df = None
 
    try:
        print("Starting gold data quality metrics pipeline...")

        # Validate dependencies
        validate_dependencies()
 
        # Read pipeline_state
        df = read_pipeline_state()
 
        if df is None:
            print("No new data to process - exiting pipeline")
            return
 
        record_count = df.count()
        print(f"Read {record_count} pipeline_state rows")
 
        if record_count == 0:
            print("Zero records after read - exiting pipeline")
            return
 
        # Get current pipeline_state version BEFORE processing
        current_version = get_last_version(
            spark, config["pipeline_state"]
        )
 
        # Compute quality metrics
        gold_df = compute_quality_metrics(df)
        print("Quality metrics computation complete")
 
        # Cache before count
        gold_df.cache()
        row_count = gold_df.count()
        print(f"Gold rows to write: {row_count}")
 
        if row_count == 0:
            print("No successful pipeline runs found - skipping write")
            # Still update state so we dont reprocess
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name=PIPELINE_NAME,
                last_processed_version=current_version,
                status="success"
            )
            return
 
        # Write
        write_to_gold(gold_df, row_count)
 
        # Update pipeline state on success
        # No good/bad counts for this pipeline itself -
        # it reads not writes records to silver
        update_pipeline_state(
            spark=spark,
            pipeline_state_table=config["pipeline_state"],
            pipeline_name=PIPELINE_NAME,
            last_processed_version=current_version,
            status="success"
        )
 
        # Optimize
        optimize_gold_table(row_count)
 
        print("Gold data quality metrics pipeline complete")
 
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        try:
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name=PIPELINE_NAME,
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
