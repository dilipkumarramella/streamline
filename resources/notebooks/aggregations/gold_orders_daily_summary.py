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
    count,
    round as spark_round,
    current_timestamp,
    countDistinct,
    lit,
    when
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
    missing = []
    if not table_exists(spark, config["silver_fact_orders"]):
        missing.append(config["silver_fact_orders"])
    if not table_exists(spark, config["silver_dim_product"]):
        missing.append(config["silver_dim_product"])
    if not table_exists(spark, config["silver_dim_customer"]):
        missing.append(config["silver_dim_customer"])
    if missing:
        raise Exception(
            f"Dependency check failed. "
            f"Missing tables: {missing}. "
            f"Ensure upstream pipelines completed successfully."
        )
    print("Dependency check passed")

# COMMAND ----------

# ─────────────────────────────────
# READ SILVER FACT ORDERS
# ─────────────────────────────────
 
def read_silver_orders():
    """
    Read fact_orders from silver layer.
    Supports two modes:
 
    Normal mode (no dates passed):
    -> Uses CDF on silver fact_orders to detect
       which order_dates have new/changed records
    -> Only those dates will be recomputed
       and overwritten in gold
 
    Backfill/Reprocessing mode (dates passed):
    -> Reads silver filtered by order date range
    -> Recomputes gold for those dates only
    -> Safe to rerun - overwrites affected partitions
 
    Returns:
        DataFrame or None
    """

    # Backfill / Reprocessing mode
    if start_date and end_date:
        print(f"Backfill/Reprocessing mode: {start_date} to {end_date}")
        df = read_data(
            spark=spark,
            file_type="delta",
            table_name=config["silver_fact_orders"]
        ).filter(
            (to_date(col("order_timestamp")) >= start_date) &
            (to_date(col("order_timestamp")) <= end_date)
        )
        return df
 
    # Normal incremental mode
    last_version = get_last_processed_version(
        spark=spark,
        pipeline_state_table=config["pipeline_state"],
        pipeline_name="silver_to_gold_orders_daily"
    )
 
    current_silver_version = get_last_version(
        spark, config["silver_fact_orders"]
    )
 
    if last_version == 0:
        # First run or no state
        # Read all silver - overwrite handles idempotency
        print("No pipeline state found - reading all silver data")
        return read_data(
            spark=spark,
            file_type="delta",
            table_name=config["silver_fact_orders"]
        )
 
    if last_version >= current_silver_version:
        print("No new versions in silver - nothing to process")
        return None
 
    # CDF to find which order_dates have changed records
    print(f"Reading CDF from silver version {last_version + 1}")
    cdf_df = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["silver_fact_orders"],
        options={
            "readChangeFeed": "true",
            "startingVersion": last_version + 1
        }
    )
 
    # Get distinct affected dates from CDF
    affected_dates = cdf_df.select(
        to_date(col("order_timestamp")).alias("order_date")
    ).distinct()
 
    affected_count = affected_dates.count()
    if affected_count == 0:
        print("CDF returned no affected dates - nothing to process")
        return None
 
    print(f"Affected order dates to recompute: {affected_count}")
 
    # Read ALL silver records for affected dates only
    # We need full-day aggregation, not just the delta
    df = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["silver_fact_orders"]
    ).join(
        affected_dates,
        to_date(col("order_timestamp")) == col("order_date"),
        how="inner"
    ).drop("order_date")
 
    return df

# COMMAND ----------

# ─────────────────────────────────
# JOIN WITH DIM PRODUCT
# ─────────────────────────────────
 
def enrich_with_product(df):
    """
    Join fact_orders with dim_product
    to get category for aggregation.
    Uses current records only (is_current = true).
    Falls back to 'unknown' if product not found
    in dim - pipeline must not break on missing dims!
 
    Args:
        df: fact_orders DataFrame
 
    Returns:
        DataFrame with category column added
    """
 
    dim_product = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["silver_dim_product"]
    ).filter(
        col("is_current") == True
    ).select(
        col("product_id"),
        col("category")
    )
 
    # Left join - orders without a matching product
    # still go to gold with category = null → filled below
    df = df.join(dim_product, on="product_id", how="left")
 
    # Fill missing category after join
    df = df.fillna({"category": "unknown"})
 
    return df

# COMMAND ----------

# ─────────────────────────────────
# JOIN WITH DIM CUSTOMER
# ─────────────────────────────────
 
def enrich_with_customer(df):
    """
    Join fact_orders with dim_customer
    to get state for aggregation.
    Uses current records only (is_current = true).
    Falls back to 'unknown' if customer not found.
 
    Args:
        df: fact_orders DataFrame (post product join)
 
    Returns:
        DataFrame with state column added
    """
    dim_customer = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["silver_dim_customer"]
    ).filter(
        col("is_current") == True
    ).select(
        col("customer_id"),
        col("state")
    )
 
    df = df.join(dim_customer, on="customer_id", how="left")
    df = df.fillna({"state": "unknown"})
 
    return df

# COMMAND ----------

# ─────────────────────────────────
# COMPUTE DAILY AGGREGATION
# ─────────────────────────────────
 
def compute_daily_summary(df):
    """
    Aggregate fact_orders to daily grain.
    Grain: (summary_date, category, state)
 
    Metrics:
    - total_orders:     distinct order_ids per grain
    - total_revenue:    sum of (quantity * unit_price) per grain
    - avg_order_value:  total_revenue / total_orders
 
    Only delivered orders count toward revenue.
    All statuses count toward total_orders.
    created_at = pipeline processing timestamp.
 
    Args:
        df: Enriched fact_orders DataFrame
 
    Returns:
        Aggregated gold DataFrame
    """
    gold_df = df.withColumn(
        "summary_date", to_date(col("order_timestamp"))
    ).withColumn(
        "line_revenue",
        when(
            col("order_status") == "delivered",
            col("quantity") * col("unit_price")
        ).otherwise(lit(0.0))
    ).groupBy(
        "summary_date",
        "category",
        "state"
    ).agg(
        countDistinct("order_id").alias("total_orders"),
        spark_round(spark_sum("line_revenue"), 2).alias("total_revenue"),
    ).withColumn(
        "avg_order_value",
        spark_round(
            col("total_revenue") / col("total_orders"), 2
        )
    ).withColumn(
        "created_at", current_timestamp()
    ).select(
        "summary_date",
        "category",
        "state",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "created_at"
    )
 
    return gold_df

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO GOLD
# ─────────────────────────────────
 
def write_to_gold(gold_df, row_count):
    """
    Write aggregated data to gold.orders_daily_summary.
    Strategy: overwrite by date partition (idempotent).
    Each affected summary_date is fully recomputed
    and overwritten - safe to rerun!
 
    First run: creates external table with location.
    Subsequent runs: dynamic partition overwrite
    to replace only affected date partitions.
 
    Args:
        gold_df: Aggregated gold DataFrame
        row_count: Row count (skip write if 0)
    """
    if row_count == 0:
        print("No rows to write - skipping")
        return
 
    # Enable dynamic partition overwrite
    # So only affected date partitions are replaced
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode", "dynamic"
    )
 
    if not table_exists(spark, config["gold_orders_daily"]):
        # First run - create external table
        print(f"First run - creating {config['gold_orders_daily']}")
        write_data(
            spark=spark,
            df=gold_df,
            file_type="delta",
            mode="overwrite",
            table_name=config["gold_orders_daily"],
            location=get_table_location(
                config["gold_path"],
                config["gold_orders_daily"]
                )
        )
 
    else:
        # Overwrite affected date partitions only
        write_data(
            spark=spark,
            df=gold_df,
            file_type="delta",
            mode="overwrite",
            table_name=config["gold_orders_daily"],
            options={"partitionOverwriteMode": "dynamic"}
        )
 
    print(f"{row_count} rows written to {config['gold_orders_daily']}")

# COMMAND ----------

# ─────────────────────────────────
# OPTIMIZE
# ─────────────────────────────────
 
def optimize_gold_table(row_count):
    """
    Run OPTIMIZE on gold orders_daily_summary.
    ZORDER by summary_date + category for
    fast dashboard queries.
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
            table_name=config["gold_orders_daily"],
            zorder_cols=["summary_date", "category"]
        )
        print(f"Optimize complete on {config['gold_orders_daily']}")
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
    2. Read silver fact_orders (CDF incremental)
    3. Enrich with dim_product (category)
    4. Enrich with dim_customer (state)
    5. Aggregate to daily grain
    6. Overwrite affected date partitions in gold
    7. Update pipeline state
    8. Optimize
    """
    try:
        print("Starting gold orders daily summary pipeline...")

        # Validate dependencies
        validate_dependencies()
 
        # Read silver
        df = read_silver_orders()
 
        if df is None:
            print("No new data to process - exiting pipeline")
            return
 
        record_count = df.count()
        print(f"Read {record_count} records from silver fact_orders")
 
        if record_count == 0:
            print("Zero records after read - exiting pipeline")
            return
 
        # Get current silver version BEFORE processing
        current_version = get_last_version(
            spark, config["silver_fact_orders"]
        )
 
        # Enrich with dims
        df = enrich_with_product(df)
        df = enrich_with_customer(df)
        print("Dimension enrichment complete")
 
        # Aggregate
        gold_df = compute_daily_summary(df)
        print("Daily aggregation complete")
 
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
            pipeline_name="silver_to_gold_orders_daily",
            last_processed_version=current_version,
            status="success"
        )
 
        # Optimize
        optimize_gold_table(row_count)
 
        print("Gold orders daily summary pipeline complete")
 
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        try:
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="silver_to_gold_orders_daily",
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
