from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType,
    IntegerType, DoubleType,
    ArrayType, BooleanType,
    DateType, LongType
)

# Orders item schema (nested inside orders)
item_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True)
])

# Bronze orders schema
bronze_orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_timestamp", TimestampType(), True),
    StructField("order_status", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("items", ArrayType(item_schema), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("ingested_at", TimestampType(), True)
])

# Bronze payments schema
bronze_payments_schema = StructType([
    StructField("payment_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_timestamp", TimestampType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("gateway_response_code", StringType(), True),
    StructField("retry_count", IntegerType(), True),
    StructField("ingested_at", TimestampType(), True)
])

# Bronze clickstream schema
bronze_clickstream_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("device", StringType(), True),
    StructField("ingested_at", TimestampType(), True)
])

# Silver fact_orders schema
silver_orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_timestamp", TimestampType(), True),
    StructField("created_at", TimestampType(), True)
])

# Silver fact_payments schema
silver_payments_schema = StructType([
    StructField("payment_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_timestamp", TimestampType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("gateway_response_code", StringType(), True),
    StructField("retry_count", IntegerType(), True),
    StructField("created_at", TimestampType(), True)
])

# Silver fact_events schema
silver_events_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("device", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# Silver dim_customer schema
silver_dim_customer_schema = StructType([
    StructField("customer_sk", LongType(), False),
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("created_at", TimestampType(), True)
])

# Silver dim_product schema
silver_dim_product_schema = StructType([
    StructField("product_sk", LongType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("is_current", BooleanType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("created_at", TimestampType(), True)
])