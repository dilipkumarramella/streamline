# Databricks notebook source
# ─────────────────────────────────
# IMPORTS
# ─────────────────────────────────
import sys
 
sys.path.append('/Workspace/Users/dilip.dot.dot@gmail.com/streamline/')
 
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
    countDistinct,
    when,
    lit,
    current_timestamp,
    round as spark_round
)

# COMMAND ----------

# ─────────────────────────────────
# CONFIGS
# ─────────────────────────────────
# env = dbutils.widgets.get("env")
config = get_config(env="dev")
 
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
    if not table_exists(spark, config["silver_fact_events"]):
        raise Exception(
            f"Dependency check failed. "
            f"Missing table: {config['silver_fact_events']}. "
            f"Ensure upstream pipelines completed successfully."
        )
    print("Dependency check passed")

# COMMAND ----------

# ─────────────────────────────────
# READ SILVER FACT EVENTS
# ─────────────────────────────────
 
def read_silver_events():
    """
    Read fact_events from silver layer.
    Entire funnel computed from fact_events only.
 
    event_types used:
    - view:         product views (top of funnel)
    - add_to_cart:  add to cart
    - checkout:     checkout started
    - purchase:     order placed (bottom of funnel)
 
    fact_orders not used - both fact_orders and
    fact_events are independently faker-generated
    with no real relationship between them.
    purchase events in fact_events are the correct
    source for bottom-of-funnel metric.
 
    Normal mode:
    -> CDF on fact_events since last processed version
    -> Get affected dates
    -> Read full events for those dates
 
    Backfill mode:
    -> Read events filtered by date range
 
    Returns:
        DataFrame or None if nothing to process
    """
    # Backfill / Reprocessing mode
    if start_date and end_date:
        print(f"Backfill/Reprocessing mode: {start_date} to {end_date}")
        df = read_data(
            spark=spark,
            file_type="delta",
            table_name=config["silver_fact_events"]
        ).filter(
            (to_date(col("event_timestamp")) >= start_date) &
            (to_date(col("event_timestamp")) <= end_date)
        )
        return df
 
    # Normal incremental mode
    last_version = get_last_processed_version(
        spark=spark,
        pipeline_state_table=config["pipeline_state"],
        pipeline_name="silver_to_gold_funnel_metrics"
    )
 
    current_silver_version = get_last_version(
        spark, config["silver_fact_events"]
    )
 
    if last_version == 0:
        print("No pipeline state found - reading all silver data")
        return read_data(
            spark=spark,
            file_type="delta",
            table_name=config["silver_fact_events"]
        )
 
    if last_version >= current_silver_version:
        print("No new versions in silver - nothing to process")
        return None
 
    # CDF to find affected dates
    print(f"Reading CDF from silver version {last_version + 1}")
    cdf_df = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["silver_fact_events"],
        options={
            "readChangeFeed": "true",
            "startingVersion": last_version + 1
        }
    )
 
    affected_dates = cdf_df.select(
        to_date(col("event_timestamp")).alias("event_date")
    ).distinct()
 
    affected_count = affected_dates.count()
    if affected_count == 0:
        print("No affected dates found - nothing to process")
        return None
 
    print(f"Affected event dates to recompute: {affected_count}")
 
    # Read ALL events for affected dates
    # Full day needed for correct funnel aggregation
    df = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["silver_fact_events"]
    ).join(
        affected_dates,
        to_date(col("event_timestamp")) == col("event_date"),
        how="inner"
    ).drop("event_date")
 
    return df

# COMMAND ----------

# ─────────────────────────────────
# COMPUTE FUNNEL METRICS
# ─────────────────────────────────
 
def compute_funnel_metrics(df):
    """
    Compute all 4 funnel stages from fact_events.
    Grain: event_date
 
    All stages use countDistinct(session_id)
    per event_type per day.
 
    session_id is the correct grain:
    - Same customer two sessions = two funnel entries
    - Avoids double counting within same session
 
    Stages:
    - product_views:    event_type = 'view'
    - add_to_cart:      event_type = 'add_to_cart'
    - checkout_started: event_type = 'checkout'
    - orders_placed:    event_type = 'purchase'
 
    conversion_rate = orders_placed / product_views
    - 0.0 if product_views = 0 (avoid divide by zero)
    - Rounded to 4 decimal places
 
    Args:
        df: silver fact_events DataFrame
 
    Returns:
        Final gold funnel_metrics DataFrame
    """
    gold_df = df.withColumn(
        "event_date", to_date(col("event_timestamp"))
    ).groupBy("event_date").agg(
        countDistinct(
            when(col("event_type") == "view",
                 col("session_id"))
        ).alias("product_views"),
        countDistinct(
            when(col("event_type") == "add_to_cart",
                 col("session_id"))
        ).alias("add_to_cart"),
        countDistinct(
            when(col("event_type") == "checkout",
                 col("session_id"))
        ).alias("checkout_started"),
        countDistinct(
            when(col("event_type") == "purchase",
                 col("session_id"))
        ).alias("orders_placed")
    ).withColumn(
        "conversion_rate",
        when(
            col("product_views") == 0, lit(0.0)
        ).otherwise(
            spark_round(
                col("orders_placed") / col("product_views"), 4
            )
        )
    ).withColumn(
        "created_at", current_timestamp()
    ).select(
        "event_date",
        "product_views",
        "add_to_cart",
        "checkout_started",
        "orders_placed",
        "conversion_rate",
        "created_at"
    )
 
    return gold_df

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO GOLD
# ─────────────────────────────────
 
def write_to_gold(gold_df, row_count):
    """
    Write funnel metrics to gold.funnel_metrics.
    Strategy: dynamic partition overwrite by event_date.
    Each affected event_date fully recomputed
    and overwritten - safe to rerun!
 
    Args:
        gold_df: Final funnel metrics DataFrame
        row_count: Skip write if 0
    """
    if row_count == 0:
        print("No rows to write - skipping")
        return
 
    gold_location = get_table_location(
        config["gold_path"],
        config["gold_funnel_metrics"]
    )
 
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode", "dynamic"
    )
 
    if not table_exists(spark, config["gold_funnel_metrics"]):
        print(f"First run - creating {config['gold_funnel_metrics']}")
        write_data(
            spark=spark,
            df=gold_df,
            file_type="delta",
            mode="overwrite",
            table_name=config["gold_funnel_metrics"],
            location=gold_location
        )
    else:
        write_data(
            spark=spark,
            df=gold_df,
            file_type="delta",
            mode="overwrite",
            table_name=config["gold_funnel_metrics"],
            options={"partitionOverwriteMode": "dynamic"}
        )
 
    print(f"{row_count} rows written to {config['gold_funnel_metrics']}")

# COMMAND ----------

# ─────────────────────────────────
# OPTIMIZE
# ─────────────────────────────────
 
def optimize_gold_table(row_count):
    """
    Run OPTIMIZE on gold funnel_metrics.
    ZORDER by event_date for fast
    date-range dashboard queries.
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
            table_name=config["gold_funnel_metrics"],
            zorder_cols=["event_date"]
        )
        print(f"Optimize complete on {config['gold_funnel_metrics']}")
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
    2. Read silver fact_events (CDF incremental)
    3. Compute all 4 funnel stages from events only
    4. Compute conversion_rate
    5. Overwrite affected date partitions in gold
    6. Update pipeline state
    7. Optimize
    """
    gold_df = None
 
    try:
        print("Starting gold funnel metrics pipeline...")

        # Validate dependencies
        validate_dependencies()
 
        # Read silver events
        df = read_silver_events()
 
        if df is None:
            print("No new data to process - exiting pipeline")
            return
 
        record_count = df.count()
        print(f"Read {record_count} records from silver fact_events")
 
        if record_count == 0:
            print("Zero records after read - exiting pipeline")
            return
 
        # Get current silver version BEFORE processing
        current_version = get_last_version(
            spark, config["silver_fact_events"]
        )
 
        # Compute funnel metrics
        gold_df = compute_funnel_metrics(df)
        print("Funnel metrics computation complete")
 
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
            pipeline_name="silver_to_gold_funnel_metrics",
            last_processed_version=current_version,
            status="success"
        )
 
        # Optimize
        optimize_gold_table(row_count)
 
        print("Gold funnel metrics pipeline complete")
 
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        try:
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="silver_to_gold_funnel_metrics",
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
