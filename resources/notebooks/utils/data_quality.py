from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit, current_timestamp, row_number
from pyspark.sql.window import Window


# ---------------------------------
# DATA QUALITY FUNCTIONS
# ---------------------------------

def check_nulls(
    df,
    columns: list
) -> DataFrame:
    """
    Check for null values in specified columns.
    Adds rejection_reason column to DataFrame.

    Args:
        df: Input DataFrame
        columns: List of columns to check for nulls
                 Example: ["order_id", "customer_id"]

    Returns:
        DataFrame with rejection_reason column added

    Example:
        df = check_nulls(
            df=orders_df,
            columns=["order_id", "customer_id", "order_timestamp"]
        )
    """
    # Add rejection_reason column if not exists
    if "rejection_reason" not in df.columns:
        df = df.withColumn("rejection_reason", lit(None).cast("string"))

    # Check each column for nulls
    for column in columns:
        df = df.withColumn(
            "rejection_reason",
            when(
                col(column).isNull() & col("rejection_reason").isNull(),
                lit(f"null_{column}")
            ).otherwise(col("rejection_reason"))
        )

    return df


def check_duplicates(
    df,
    partition_cols: list,
    sort_col: str
) -> DataFrame:
    """
    Check for duplicate records using window function.
    Keeps latest record as good record!
    Marks older duplicates with rejection_reason.

    Args:
        df: Input DataFrame
        partition_cols: Columns to check duplicates on
                        Example: ["order_id"]
                        Multiple: ["order_id", "product_id"]
        sort_col: Column to sort by to find latest record
                  Example: "order_timestamp"
                  Latest record = row_num 1 = good ✅
                  Older duplicates = row_num > 1 = bad ✅

    Returns:
        DataFrame with rejection_reason column updated

    Example:
        df = check_duplicates(
            df=orders_df,
            partition_cols=["order_id"],
            sort_col="order_timestamp"
        )
    """

    # Add rejection_reason if not exists
    if "rejection_reason" not in df.columns:
        df = df.withColumn("rejection_reason", lit(None).cast("string"))

    # Build window spec
    window = Window \
        .partitionBy(partition_cols) \
        .orderBy(col(sort_col).desc())

    # Add row number
    df = df.withColumn("row_num", row_number().over(window))

    # Mark duplicates
    df = df.withColumn(
        "rejection_reason",
        when(
            (col("row_num") > 1) & col("rejection_reason").isNull(),
            lit(f"duplicate_{'_'.join(partition_cols)}")
        ).otherwise(col("rejection_reason"))
    )

    # Drop helper column
    df = df.drop("row_num")

    return df


    def check_positive_values(
    df,
    columns: list
) -> DataFrame:
    """
    Check for non positive values in specified columns.
    Zero and negative values are invalid!

    Args:
        df: Input DataFrame
        columns: List of columns to check
                 Example: ["unit_price", "quantity"]

    Returns:
        DataFrame with rejection_reason column updated

    Example:
        df = check_positive_values(
            df=orders_df,
            columns=["unit_price", "quantity"]
        )
    """
    # Add rejection_reason if not exists
    if "rejection_reason" not in df.columns:
        df = df.withColumn("rejection_reason", lit(None).cast("string"))

    for column in columns:
        df = df.withColumn(
            "rejection_reason",
            when(
                (col(column) <= 0) & col("rejection_reason").isNull(),
                lit(f"non_positive_{column}")
            ).otherwise(col("rejection_reason"))
        )

    return df


def check_valid_values(
    df,
    valid_values_map: dict
) -> DataFrame:
    """
    Check if column values are within valid values list.
    Invalid values are marked with rejection_reason.

    Args:
        df: Input DataFrame
        valid_values_map: Dictionary of column to valid values
                          Example: {
                              "order_status": ["delivered", "pending",
                                               "cancelled", "returned"],
                              "payment_status": ["success", "failed",
                                                 "pending"]
                          }

    Returns:
        DataFrame with rejection_reason column updated

    Example:
        df = check_valid_values(
            df=orders_df,
            valid_values_map={
                "order_status": ["delivered", "pending",
                                 "cancelled", "returned"],
                "payment_status": ["success", "failed", "pending"]
            }
        )
    """
    # Add rejection_reason if not exists
    if "rejection_reason" not in df.columns:
        df = df.withColumn("rejection_reason", lit(None).cast("string"))

    for column, valid_values in valid_values_map.items():
        df = df.withColumn(
            "rejection_reason",
            when(
                (~col(column).isin(valid_values)) & col("rejection_reason").isNull(),
                lit(f"invalid_{column}")
            ).otherwise(col("rejection_reason"))
        )

    return df


def check_future_dates(
    df,
    columns: list
) -> DataFrame:
    """
    Check for future dates in specified columns.
    Future dates are invalid!

    Args:
        df: Input DataFrame
        columns: List of date/timestamp columns to check
                 Example: ["order_timestamp", "ingested_at",
                           "created_at"]

    Returns:
        DataFrame with rejection_reason column updated

    Example:
        df = check_future_dates(
            df=orders_df,
            columns=["order_timestamp", "ingested_at"]
        )
    """
    # Add rejection_reason if not exists
    if "rejection_reason" not in df.columns:
        df = df.withColumn("rejection_reason", lit(None).cast("string"))

    for column in columns:
        df = df.withColumn(
            "rejection_reason",
            when(
                (col(column) > current_timestamp()) & col("rejection_reason").isNull(),
                lit(f"future_date_{column}")
            ).otherwise(col("rejection_reason"))
        )

    return df


def quarantine_records(
    df
) -> tuple:
    """
    Split DataFrame into good and bad records
    based on rejection_reason column.

    Good records → rejection_reason is null → Silver ✅
    Bad records → rejection_reason is not null → Quarantine ✅

    Args:
        df: Input DataFrame with rejection_reason column

    Returns:
        tuple: (good_df, bad_df)
               good_df = clean records, rejection_reason dropped
               bad_df = rejected records, rejection_reason kept

    Example:
        good_df, bad_df = quarantine_records(df=orders_df)

        # Write good records to Silver
        merge_to_delta(
            spark=spark,
            source_df=good_df,
            target_table=config["silver_fact_orders"],
            ...
        )

        # Write bad records to Quarantine
        write_data(
            df=bad_df,
            file_type="delta",
            table_name=config["silver_orders_quarantine"]
        )
    """
    # Good records = rejection_reason is null
    # Drop rejection_reason before going to Silver
    good_df = df.filter(
        col("rejection_reason").isNull()
    ).drop("rejection_reason")

    # Bad records = rejection_reason is not null
    # Keep rejection_reason for quarantine
    bad_df = df.filter(
        col("rejection_reason").isNotNull()
    )

    return good_df, bad_df