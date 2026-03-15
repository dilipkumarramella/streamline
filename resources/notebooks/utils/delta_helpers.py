from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_date, lit
from delta.tables import DeltaTable

# ---------------------------------
# READ FUNCTIONS
# ---------------------------------
def read_data(
    spark,
    file_type: str,
    location: str = None,
    schema=None,
    options: dict = {},
    table_name: str = None
) -> DataFrame:
    """
    Reusable function to read any batch data source.
    
    Args:
        spark: SparkSession
        file_type: "json", "csv", "delta", "parquet"
        location: ADLS path (optional)
                  Default: None
                  Example: "abfss://bronze@storage.net/"
        schema: StructType schema (optional)
                Default: None (Spark infers)
        options: Extra spark read options
                 Default: empty dict
                 Example: {"header": "true"}
        table_name: Unity Catalog table name (optional)
                    Default: None
                    Example: "streamline.bronze.orders"
                    Use this OR location, not both!
    
    Returns:
        DataFrame
    
    Example:
        # Read by path:
        orders_df = read_data(
            spark=spark,
            file_type="delta",
            location=config["bronze_path"]
        )
        
        # Read by table name:
        orders_df = read_data(
            spark=spark,
            file_type="delta",
            table_name=config["bronze_orders"]
        )
    """
    reader = spark.read.format(file_type)
    
    if schema:
        reader = reader.schema(schema)
    
    for key, value in options.items():
        reader = reader.option(key, value)
    
    if table_name:
        return reader.table(table_name)
    elif location:
        return reader.load(location)


def read_stream_data(
    spark,
    file_type: str,
    location: str = None,
    schema=None,
    options: dict = {}
) -> DataFrame:
    """
    Reusable function to read streaming data.
    
    Args:
        spark: SparkSession
        file_type: "delta", "parquet", "json", "kafka"
        location: ADLS path (optional)
                  Default: None
                  Not needed for Kafka ✅
        schema: StructType schema (optional)
                Default: None
        options: Extra spark readStream options
                 Default: empty dict
                 Example: {"maxFilesPerTrigger": 1}
                 For Kafka: {"kafka.bootstrap.servers": "xxx",
                             "subscribe": "orders-raw"}
    
    Returns:
        Streaming DataFrame
    
    Example:
        # Delta streaming:
        orders_stream_df = read_stream_data(
            spark=spark,
            file_type="delta",
            location=config["bronze_path"]
        )
        
        # Kafka streaming:
        orders_kafka_df = read_stream_data(
            spark=spark,
            file_type="kafka",
            options={
                "kafka.bootstrap.servers": "xxx",
                "subscribe": "orders-raw",
                "startingOffsets": "latest"
            }
        )
    """
    reader = spark.readStream.format(file_type)

    if schema:
        reader = reader.schema(schema)

    for key, value in options.items():
        reader = reader.option(key, value)

    if location:
        return reader.load(location)
    else:
        return reader.load()
    

# -------------------------------------
# WRITE FUNCTIONS
# -------------------------------------

def write_data(
    df,
    file_type: str,
    mode: str = "append",
    location: str = None,
    table_name: str = None,
    options: dict = {}
) -> None:
    """
    Reusable function to write DataFrame
    to any data format.

    Args:
        df: Source DataFrame to write
        file_type: "delta", "parquet", "csv"
        mode: Write mode
              Default: "append"
              Options: "append", "overwrite"
        location: ADLS path (optional)
                  Default: None
                  Example: "abfss://bronze@storage.net/"
        table_name: Unity Catalog table name (optional)
                    Default: None
                    Example: "streamline.bronze.orders"
                    Use this OR location, not both!
        options: Extra spark write options
                 Default: empty dict
                 Example: {"header": "true"}

    Returns:
        None

    Example:
        # Write by table name:
        write_data(
            df=orders_df,
            file_type="delta",
            table_name=config["bronze_orders"]
        )

        # Write by path:
        write_data(
            df=orders_df,
            file_type="delta",
            location=config["bronze_path"],
            mode="overwrite"
        )
    """
    writer = df.write.format(file_type)

    writer = writer.mode(mode)
    
    # Always merge schema for schema evolution
    writer = writer.option("mergeSchema", "true")
    
    for key, value in options.items():
        writer = writer.option(key, value)

    if table_name:
        writer.saveAsTable(table_name)
    elif location:
        writer.save(location)


def merge_to_delta(
    spark,
    source_df,
    target_table: str,
    merge_condition: str,
    update_set: dict,
) -> None:
    """
    Idempotent MERGE into Delta table.
    Prevents duplicates on rerun!

    Args:
        spark: SparkSession
        source_df: New/updated data DataFrame
        target_table: Target Delta table name
                      Example: "streamline.silver.fact_orders"
        merge_condition: How to match rows
                         Example: "target.order_id = source.order_id"
        update_set: Columns to update when matched
                    Example: {"order_status": "source.order_status",
                              "updated_at": "source.updated_at"}

    Returns:
        None

    Example:
        merge_to_delta(
            spark=spark,
            source_df=clean_orders_df,
            target_table=config["silver_fact_orders"],
            merge_condition="target.order_id = source.order_id",
            update_set={
                "order_status": "source.order_status",
                "updated_at": "source.updated_at"
            }
        )
    """
    target = DeltaTable.forName(spark, target_table)

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    target.alias("target") \
        .merge(
            source_df.alias("source"),
            merge_condition
        ) \
        .whenMatchedUpdate(set=update_set) \
        .whenNotMatchedInsertAll() \
        .execute()


def scd2_merge(
    spark,
    source_df,
    target_table: str,
    natural_key: str,
    tracked_columns: list
) -> None:
    """
    SCD Type 2 MERGE into Delta dimension table.
    Closes old record and inserts new record
    when tracked columns change.

    Args:
        spark: SparkSession
        source_df: New/updated dimension data
        target_table: Target Delta dimension table
                      Example: "streamline.silver.dim_customer"
        natural_key: Business key column name
                     Example: "customer_id"
        tracked_columns: Columns to track for changes
                         Example: ["city", "customer_name"]

    Returns:
        None

    Example:
        scd2_merge(
            spark=spark,
            source_df=customers_df,
            target_table=config["silver_dim_customer"],
            natural_key="customer_id",
            tracked_columns=["city", "customer_name"]
        )
    """
    from pyspark.sql.functions import current_date, lit
    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, target_table)

    # Step 1: Build change detection condition
    # Check if any tracked column has changed
    change_condition = " OR ".join([
        f"target.{col} != source.{col}"
        for col in tracked_columns
    ])

    # Step 2: Close existing current records
    # where natural key matches AND data changed
    target.alias("target") \
        .merge(
            source_df.alias("source"),
            f"target.{natural_key} = source.{natural_key} "
            f"AND target.is_current = true "
            f"AND ({change_condition})"
        ) \
        .whenMatchedUpdate(set={
            "is_current": "false",
            "end_date": "current_date()"
        }) \
        .execute()

    # Step 3: Insert new records
    # Only for changed or new customers
    new_records_df = source_df.join(
        target.toDF().filter("is_current = true"),
        on=natural_key,
        how="left_anti"
    ).union(
        source_df.join(
            target.toDF().filter("is_current = true"),
            on=[natural_key] + tracked_columns,
            how="left_anti"
        )
    ).distinct()

    new_records_df \
        .withColumn("is_current", lit(True)) \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit(None).cast("date")) \
        .write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(target_table)


def write_stream_data(
    df,
    file_type: str,
    checkpoint_location: str,
    output_mode: str = "append",
    table_name: str = None,
    location: str = None,
    options: dict = {}
) -> None:
    """
    Reusable function to write streaming DataFrame.
    Works for any format!

    Args:
        df: Streaming DataFrame
        file_type: "delta", "parquet", "json"
        checkpoint_location: Path to store checkpoint
                             Mandatory for exactly once! ✅
                             Example: "abfss://checkpoints@storage.net/bronze/orders/"
        output_mode: Streaming output mode
                     Default: "append"
                     Options: "append", "complete", "update"
        table_name: Unity Catalog table name (optional)
                    Default: None
                    Example: "streamline.bronze.orders"
        location: ADLS path (optional)
                  Default: None
                  Example: "abfss://bronze@storage.net/"
        options: Extra writeStream options
                 Default: empty dict

    Returns:
        None

    Example:
        # Write stream to table:
        write_stream_data(
            df=orders_stream_df,
            file_type="delta",
            checkpoint_location=config["checkpoint_path"] + "bronze/orders/",
            table_name=config["bronze_orders"]
        )
    """
    writer = df.writeStream \
        .format(file_type) \
        .outputMode(output_mode) \
        .option("checkpointLocation", checkpoint_location)

    for key, value in options.items():
        writer = writer.option(key, value)

    if table_name:
        writer.toTable(table_name)
    elif location:
        writer.start(location)


# ------------------------------------
# Maintenance Tasks
# ------------------------------------

def optimize_table(
    spark,
    table_name: str,
    zorder_cols: list = []
) -> None:
    """
    Run OPTIMIZE and ZORDER on Delta table.
    Compacts small files and sorts data
    for faster queries and lower cost!

    Args:
        spark: SparkSession
        table_name: Delta table name
                    Example: "streamline.silver.fact_orders"
        zorder_cols: Columns to ZORDER by (optional)
                     Default: empty list (just OPTIMIZE)
                     Example: ["customer_id", "order_timestamp"]
                     Choose frequently filtered columns!

    Returns:
        None

    Example:
        # With ZORDER:
        optimize_table(
            spark=spark,
            table_name=config["silver_fact_orders"],
            zorder_cols=["customer_id", "order_timestamp"]
        )

        # Without ZORDER:
        optimize_table(
            spark=spark,
            table_name=config["silver_fact_orders"]
        )
    """
    if zorder_cols:
        cols = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({cols})")
    else:
        spark.sql(f"OPTIMIZE {table_name}")


def vacuum_table(
    spark,
    table_name: str,
    retention_hours: int = 168
) -> None:
    """
    Run VACUUM on Delta table.
    Removes old files beyond retention period.
    Saves storage cost!

    Args:
        spark: SparkSession
        table_name: Delta table name
                    Example: "streamline.silver.fact_orders"
        retention_hours: Hours to retain old files
                         Default: 168 (7 days)
                         WARNING: Never go below 168!
                         Databricks recommendation

    Returns:
        None

    Example:
        vacuum_table(
            spark=spark,
            table_name=config["silver_fact_orders"]
        )

        # Custom retention:
        vacuum_table(
            spark=spark,
            table_name=config["silver_fact_orders"],
            retention_hours=336  # 14 days
        )
    """
    spark.sql(f"""
        VACUUM {table_name}
        RETAIN {retention_hours} HOURS
    """)


def get_last_version(
    spark,
    table_name: str
) -> int:
    """
    Get latest committed version of Delta table.
    Used for CDF incremental reads in batch pipeline!

    Args:
        spark: SparkSession
        table_name: Delta table name
                    Example: "streamline.bronze.orders"

    Returns:
        int: Latest version number

    Example:
        last_version = get_last_version(
            spark=spark,
            table_name=config["bronze_orders"]
        )

        # Use in incremental read:
        df = read_data(
            spark=spark,
            file_type="delta",
            table_name=config["bronze_orders"],
            options={
                "readChangeFeed": "true",
                "startingVersion": last_version
            }
        )
    """
    return DeltaTable.forName(
        spark, table_name
    ).history(1).select("version").collect()[0][0]


def table_exists(
    spark,
    table_name: str
) -> bool:
    """
    Check if Delta table exists in Unity catalog
    Used before first pipeline run to decide
    between write_data() and merge_to_delta()!

    Args:
        spark: SparkSession
        table_name: Unity Catalog table name
                    Example: "streamline.silver.fact_orders"

    Returns:
        bool: True if exists
              False if not exists

    Example:
        if not table_exists(spark, config["silver_fact_orders"]):
            # First run = table doesnt exist
            # Simple write to create table
            write_data(
                df=clean_df,
                file_type="delta",
                table_name=config["silver_fact_orders"]
            )
        else:
            # Table exists = incremental merge
            merge_to_delta(
                spark=spark,
                source_df=clean_df,
                target_table=config["silver_fact_orders"],
                ...
            )
    """
    return spark.catalog.tableExists(table_name)