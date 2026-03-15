# Databricks notebook source
# ─────────────────────────────────
# IMPORTS
# ─────────────────────────────────
import sys
import os

sys.path.append('/Workspace/Users/dilip.dot.dot@gmail.com/streamline/')

from resources.notebooks.utils.config import get_config
from resources.notebooks.utils.delta_helpers import (
    read_data,
    write_data,
    merge_to_delta,
    optimize_table,
    table_exists,
    get_last_version
)
from resources.notebooks.utils.data_quality import (
    check_nulls,
    check_duplicates,
    check_positive_values,
    check_valid_values,
    check_future_dates,
    quarantine_records
)

from pyspark.sql.functions import (
    col,
    explode,
    trim,
    current_timestamp,
    expr
)


# COMMAND ----------

spark.version

# COMMAND ----------

# ─────────────────────────────────
# CONFIGS
# ─────────────────────────────────
# env = dbutils.widgets.get("env")
config = get_config(env="dev")

# COMMAND ----------

# ─────────────────────────────────
# READ BRONZE ORDERS WITH CDF
# ─────────────────────────────────

def read_bronze_orders():
    """
    Read new records from bronze.orders
    using Change Data Feed (CDF).
    Only reads records since last version!
    Incremental read!
    """
    if table_exists(spark, config["silver_fact_orders"]):
        # Get last processed version
        last_version = get_last_version(
            spark,
            config["bronze_orders"]
        )

        # Read only new records using CDF
        df = read_data(
            spark=spark,
            file_type="delta",
            table_name=config["bronze_orders"],
            options={
                "readChangeFeed": "true",
                "startingVersion": last_version
            }
        )
    else:
        # First run!
        # Read everything from bronze
        df = read_data(
            spark=spark,
            file_type="delta",
            table_name=config["bronze_orders"]
        )

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
        "category"         # goes to dim_product
        "ingested_at"      # bronze only
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
    # Cast all columns to correct types
    df = df.withColumn("order_id", expr("try_cast(order_id as string)"))
    df = df.withColumn("customer_id", expr("try_cast(customer_id as string)"))
    df = df.withColumn("product_id", expr("try_cast(product_id as string)"))
    df = df.withColumn("order_status", expr("try_cast(order_status as string)"))
    df = df.withColumn("order_timestamp", expr("try_cast(order_timestamp as timestamp)"))
    df = df.withColumn("quantity", expr("try_cast(quantity as int)"))
    df = df.withColumn("unit_price", expr("try_cast(unit_price as double)"))
    
    # Trim String columns
    df = df.withColumn("order_id", trim(col("order_id")))
    df = df.withColumn("customer_id", trim(col("customer_id")))
    df = df.withColumn("product_id", trim(col("product_id")))
    df = df.withColumn("order_status", trim(col("order_status")))

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
                "Unknown"
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

def write_to_silver(good_df, bad_df):
    """
    Write good records to silver.fact_orders
    Write bad records to silver.orders_quarantine
    """
    # Write good records
    if table_exists(spark, config["silver_fact_orders"]):
        # Table exists → incremental merge
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
        # First run → simple write
        write_data(
            df=good_df,
            file_type="delta",
            table_name=config["silver_fact_orders"]
        )

    # Write bad records to quarantine
    write_data(
        df=bad_df,
        file_type="delta",
        table_name=config["silver_orders_quarantine"]
    )

    print("Write complete")

# COMMAND ----------

# ─────────────────────────────────
# OPTIMIZE
# ─────────────────────────────────

def optimize_silver_tables(good_count, bad_count):
    """
    Run OPTIMIZE on silver tables after write.
    Only optimizes if records were written!
    """
    if good_count > 0:
        optimize_table(
            spark=spark,
            table_name=config["silver_fact_orders"],
            zorder_cols=["customer_id", "order_timestamp"]
        )
        print("Optimize complete on silver.fact_orders")

    if bad_count > 0:
        optimize_table(
            spark=spark,
            table_name=config["silver_orders_quarantine"]
        )
        print("Optimize complete on silver.orders_quarantine")

    if good_count == 0 and bad_count == 0:
        print("No records written - skipping optimize")

# COMMAND ----------

# ─────────────────────────────────
# PIPELINE
# ─────────────────────────────────

def run_pipeline():
    """
    Main pipeline function.
    Orchestrates all steps in order.
    """
    print("Starting silver orders pipeline...")

    # Read
    df = read_bronze_orders()
    print(f"Read {df.count()} records from bronze")

    # Transform
    df = flatten_orders(df)
    df = drop_unnecessary_columns(df)
    df = add_audit_columns(df)
    df = clean_data(df)
    print("Transformations complete")

    # Quality checks
    df = run_quality_checks(df)
    print("Quality checks complete")

    # Separate good and bad
    good_df, bad_df = quarantine_records(df)
    good_count = good_df.count()
    bad_count = bad_df.count()
    print(f"Good records: {good_count}")
    print(f"Bad records: {bad_count}")

    # Write
    write_to_silver(good_df, bad_df)
    print(f"{good_count} records written to {config['silver_fact_orders']}")
    print(f"{bad_count} records written to {config['silver_orders_quarantine']}")

    # Optimize
    optimize_silver_tables(good_count, bad_count)

    print("Silver orders pipeline complete")

# COMMAND ----------

# ─────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────
run_pipeline()
