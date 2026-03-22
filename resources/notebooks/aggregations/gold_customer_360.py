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
    merge_to_delta,
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
    sum as spark_sum,
    round as spark_round,
    min as spark_min,
    max as spark_max,
    current_timestamp,
    datediff,
    current_date,
    when,
    lit
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
    missing = []
    if not table_exists(spark, config["silver_fact_orders"]):
        missing.append(config["silver_fact_orders"])
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
# GET AFFECTED CUSTOMERS
# ─────────────────────────────────
 
def get_affected_customers():
    """
    Find customer_ids that need RFM recomputation.
 
    Incremental strategy:
    -> CDF on fact_orders since last processed version
    -> Extract distinct customer_ids from changed records
    -> Only recompute RFM for those customers
    -> All other customers in gold remain unchanged
 
    Why CDF on fact_orders not fact_payments:
    -> RFM is order-based (Recency/Frequency/Monetary
       all derived from orders not payments)
    -> Payment table drives payment_success pipeline
 
    Backfill mode:
    -> Get all customers who ordered in date range
    -> Recompute RFM for those customers only
 
    First run:
    -> Returns all customer_ids from fact_orders
    -> Full initial load
 
    Returns:
        DataFrame with single column "customer_id"
        Returns None if nothing to process
    """
    # Backfill / Reprocessing mode
    if start_date and end_date:
        print(f"Backfill/Reprocessing mode: {start_date} to {end_date}")
        affected_customers = read_data(
            spark=spark,
            file_type="delta",
            table_name=config["silver_fact_orders"]
        ).filter(
            (to_date(col("order_timestamp")) >= start_date) &
            (to_date(col("order_timestamp")) <= end_date)
        ).select("customer_id").distinct()
        return affected_customers
 
    # Normal incremental mode
    last_version = get_last_processed_version(
        spark=spark,
        pipeline_state_table=config["pipeline_state"],
        pipeline_name="silver_to_gold_customer_360"
    )
 
    current_silver_version = get_last_version(
        spark, config["silver_fact_orders"]
    )
 
    if last_version == 0:
        print("No pipeline state found - full initial load")
        affected_customers = read_data(
            spark=spark,
            file_type="delta",
            table_name=config["silver_fact_orders"]
        ).select("customer_id").distinct()
        return affected_customers
 
    if last_version >= current_silver_version:
        print("No new versions in silver - nothing to process")
        return None
 
    # CDF to find customers with new/changed orders
    print(f"Reading CDF from silver version {last_version + 1}")
    affected_customers = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["silver_fact_orders"],
        options={
            "readChangeFeed": "true",
            "startingVersion": last_version + 1
        }
    ).select("customer_id").distinct()
 
    return affected_customers

# COMMAND ----------

# ──────────────────────────────────────────────
# READ ALL TIME ORDERS FOR AFFECTED CUSTOMERS
# ──────────────────────────────────────────────
 
def read_orders_for_customers(affected_customers):
    """
    Read ALL historical orders for affected customers.
 
    Critical: RFM needs full order history per customer
    not just new orders. A customer who ordered 10 times
    before and places 1 new order needs all 11 orders
    to compute correct Frequency and Monetary values.
 
    We identified WHICH customers changed via CDF.
    Now we read their COMPLETE history from silver.
 
    Only delivered orders count toward RFM metrics -
    cancelled/returned orders did not generate real
    revenue or represent completed purchase behaviour.
 
    Args:
        affected_customers: DataFrame with customer_id column
 
    Returns:
        DataFrame of all delivered orders for
        affected customers
    """
    all_orders = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["silver_fact_orders"]
    ).filter(
        col("order_status") == "delivered"
    )
 
    # Filter to affected customers only
    orders_df = all_orders.join(
        affected_customers,
        on="customer_id",
        how="inner"
    )
 
    return orders_df

# COMMAND ----------

# ─────────────────────────────────
# READ CURRENT CUSTOMER ATTRIBUTES
# ─────────────────────────────────
 
def read_customer_attributes(affected_customers):
    """
    Read current customer attributes from dim_customer.
    Uses is_current = true to get latest name/state.
    SCD2 dim may have multiple rows per customer -
    we only want the active record.
 
    Falls back gracefully if dim_customer missing.
    customer_name/state will be null in gold
    but pipeline will not break.
 
    Args:
        affected_customers: DataFrame with customer_id column
 
    Returns:
        DataFrame with (customer_id, customer_name,
                        state) for affected customers
        Returns None if dim_customer missing
    """
    dim_df = read_data(
        spark=spark,
        file_type="delta",
        table_name=config["silver_dim_customer"]
    ).filter(
        col("is_current") == True
    ).select(
        "customer_id",
        "customer_name",
        "state"
    ).join(
        affected_customers,
        on="customer_id",
        how="inner"
    )
 
    return dim_df

# COMMAND ----------

# ─────────────────────────────────
# COMPUTE RFM BASE METRICS
# ─────────────────────────────────
 
def compute_rfm_base(orders_df):
    """
    Compute raw RFM base metrics per customer.
    Grain: customer_id
 
    R - Recency:   days since last delivered order
    F - Frequency: count of distinct delivered orders
    M - Monetary:  total revenue from delivered orders
 
    revenue per order line = quantity * unit_price
    total_revenue = sum of all order lines
 
    avg_order_value = total_revenue / total_orders
    first/last order dates for tenure context.
 
    Args:
        orders_df: Delivered orders for affected customers
 
    Returns:
        DataFrame with raw RFM metrics per customer
    """
    rfm_base = orders_df.groupBy("customer_id").agg(
        countDistinct("order_id").alias("total_orders"),
        spark_round(
            spark_sum(col("quantity") * col("unit_price")), 2
        ).alias("total_revenue"),
        spark_round(
            spark_sum(col("quantity") * col("unit_price")) /
            countDistinct("order_id"), 2
        ).alias("avg_order_value"),
        spark_min(
            to_date(col("order_timestamp"))
        ).alias("first_order_date"),
        spark_max(
            to_date(col("order_timestamp"))
        ).alias("last_order_date")
    ).withColumn(
        "recency_days",
        datediff(current_date(), col("last_order_date"))
    )
 
    return rfm_base

# COMMAND ----------

# ─────────────────────────────────
# COMPUTE RFM SEGMENTS
# ─────────────────────────────────
 
def compute_rfm_segments(rfm_base):
    """
    Assign RFM segment labels using absolute thresholds.
 
    Absolute thresholds used instead of ntile scoring
    because ntile produces arbitrary relative rankings
    with compressed data (1-2 days, 177 customers).
    Absolute thresholds produce meaningful segments
    regardless of data volume or time range.
 
    Segment logic:
    -> champions:  ordered >= 3 times AND revenue >= 100000
                   AND ordered within last 1 day
                   High frequency, high value, recent
    -> loyal:      ordered >= 3 times
                   Frequent buyers regardless of recency
    -> recent:     ordered within today (recency_days = 0)
                   AND total_orders <= 2
                   New but active customers
    -> at_risk:    ordered yesterday (recency_days = 1)
                   AND total_orders >= 2
                   Was active, starting to slip
    -> lost:       recency_days > 1
                   Havent ordered recently
    -> potential:  everything else
                   One order, ordered today
 
    No window functions - no partition warning.
    No relative scoring - segments stable across runs.
 
    Args:
        rfm_base: DataFrame with raw RFM metrics
 
    Returns:
        DataFrame with rfm_segment column added
    """
    rfm_segmented = rfm_base.withColumn(
        "rfm_segment",
        when(
            (col("recency_days") <= 1) &
            (col("total_orders") >= 3) &
            (col("total_revenue") >= 100000),
            lit("champions")
        ).when(
            col("total_orders") >= 3,
            lit("loyal")
        ).when(
            (col("recency_days") == 0) &
            (col("total_orders") <= 2),
            lit("recent")
        ).when(
            (col("recency_days") == 1) &
            (col("total_orders") >= 2),
            lit("at_risk")
        ).when(
            col("recency_days") > 1,
            lit("lost")
        ).otherwise(
            lit("potential")
        )
    )
 
    return rfm_segmented

# COMMAND ----------

# ─────────────────────────────────
# BUILD CUSTOMER 360
# ─────────────────────────────────
 
def build_customer_360(rfm_segmented, dim_df):
    """
    Join RFM metrics with customer attributes
    from dim_customer to build final 360 view.
 
    Left join on customer_id:
    -> Customers in orders but not in dim get
       null for name/state
    -> Pipeline does not break on missing dim records
 
    Final column order matches documented schema.
 
    Args:
        rfm_segmented: DataFrame with RFM scores + segments
        dim_df: Customer attributes or None
 
    Returns:
        Final gold customer_360 DataFrame
    """
    if dim_df is not None:
        gold_df = rfm_segmented.join(
            dim_df,
            on="customer_id",
            how="left"
        )
    else:
        # No dim available - add null attribute columns
        gold_df = rfm_segmented \
            .withColumn("customer_name", lit(None).cast("string")) \
            .withColumn("state", lit(None).cast("string"))
 
    gold_df = gold_df.withColumn(
        "created_at", current_timestamp()
    ).select(
        "customer_id",
        "customer_name",
        "state",
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "first_order_date",
        "last_order_date",
        "recency_days",
        "rfm_segment",
        "created_at"
    )
 
    return gold_df

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO GOLD
# ─────────────────────────────────
 
def write_to_gold(gold_df, row_count):
    """
    Write customer 360 to gold.customer_360.
    Strategy: merge on customer_id (incremental).
 
    Only affected customers are recomputed and merged.
    Unaffected customers remain unchanged in gold.
    Safe to rerun - merge is idempotent.
 
    First run: write_data to create table.
    Subsequent runs: merge_to_delta on customer_id.
 
    Update set covers all RFM metrics and attributes
    since any of these could change when new orders
    arrive for a customer.
 
    Args:
        gold_df: Final customer 360 DataFrame
        row_count: Skip write if 0
    """
    if row_count == 0:
        print("No rows to write - skipping")
        return

    if not table_exists(spark, config["gold_customer_360"]):
        print(f"First run - creating {config['gold_customer_360']}")
        write_data(
            spark=spark,
            df=gold_df,
            file_type="delta",
            mode="overwrite",
            table_name=config["gold_customer_360"],
            location=get_table_location(
                config["gold_path"],
                config["gold_customer_360"]
            )
        )
    else:
        merge_to_delta(
            spark=spark,
            source_df=gold_df,
            target_table=config["gold_customer_360"],
            merge_condition=(
                "target.customer_id = source.customer_id"
            ),
            update_set={
                "customer_name":    "source.customer_name",
                "state":            "source.state",
                "total_orders":     "source.total_orders",
                "total_revenue":    "source.total_revenue",
                "avg_order_value":  "source.avg_order_value",
                "first_order_date": "source.first_order_date",
                "last_order_date":  "source.last_order_date",
                "recency_days":     "source.recency_days",
                "rfm_segment":      "source.rfm_segment",
                "created_at":       "source.created_at"
            }
        )
 
    print(f"{row_count} rows written to {config['gold_customer_360']}")

# COMMAND ----------

# ─────────────────────────────────
# OPTIMIZE
# ─────────────────────────────────
 
def optimize_gold_table(row_count):
    """
    Run OPTIMIZE on gold customer_360.
    ZORDER by customer_id for fast
    per-customer lookups from dashboards.
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
            table_name=config["gold_customer_360"],
            zorder_cols=["customer_id", "rfm_segment"]
        )
        print(f"Optimize complete on {config['gold_customer_360']}")
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
    2. Find affected customer_ids via CDF on fact_orders
    3. Read ALL historical delivered orders for those customers
    4. Read current attributes from dim_customer
    5. Compute raw RFM metrics (R/F/M base values)
    6. Score and segment customers (absolute thresholds)
    7. Join with customer attributes
    8. Merge into gold.customer_360
    9. Update pipeline state
    10. Optimize
    """
    gold_df            = None
    affected_customers = None
 
    try:
        print("Starting gold customer 360 pipeline...")

        # Validate dependencies
        validate_dependencies()
 
        # Get affected customer_ids via CDF
        affected_customers = get_affected_customers()
 
        if affected_customers is None:
            print("No new data to process - exiting pipeline")
            return
 
        # Cache - reused for orders read + dim read
        affected_customers.cache()
        customer_count = affected_customers.count()
 
        if customer_count == 0:
            print("No affected customers found - exiting pipeline")
            return
 
        print(f"Affected customers to recompute: {customer_count}")
 
        # Get current silver version BEFORE processing
        current_version = get_last_version(
            spark, config["silver_fact_orders"]
        )
 
        # Read all time orders for affected customers
        orders_df = read_orders_for_customers(affected_customers)
        orders_count = orders_df.count()
        print(f"Delivered orders for affected customers: {orders_count}")
 
        if orders_count == 0:
            print("No delivered orders found - exiting pipeline")
            # Still update state so we dont reprocess
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="silver_to_gold_customer_360",
                last_processed_version=current_version,
                status="success"
            )
            return
 
        # Read customer attributes from dim
        dim_df = read_customer_attributes(affected_customers)
 
        # Compute RFM
        rfm_base      = compute_rfm_base(orders_df)
        rfm_segmented = compute_rfm_segments(rfm_base)
        print("RFM computation complete")
 
        # Build final 360 view
        gold_df = build_customer_360(rfm_segmented, dim_df)
        print("Customer 360 build complete")
 
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
            pipeline_name="silver_to_gold_customer_360",
            last_processed_version=current_version,
            status="success"
        )
 
        # Optimize
        optimize_gold_table(row_count)
 
        print("Gold customer 360 pipeline complete")
 
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        try:
            update_pipeline_state(
                spark=spark,
                pipeline_state_table=config["pipeline_state"],
                pipeline_name="silver_to_gold_customer_360",
                last_processed_version=0,
                status="failed"
            )
        except Exception as state_error:
            print(f"State update failed: {str(state_error)}")
        raise
 
    finally:
        try:
            affected_customers.unpersist()
        except:
            pass
        try:
            gold_df.unpersist()
        except:
            pass

# COMMAND ----------

# ─────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────
run_pipeline()
