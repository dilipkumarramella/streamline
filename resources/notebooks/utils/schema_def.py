from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType,
    IntegerType, DoubleType,
    ArrayType, BooleanType,
    DateType, LongType
)

# Orders item schema (nested inside orders)
item_schema = StructType([
    StructField("product_id",StringType(),True),
    StructField("product_name",StringType(),True),
    StructField("category",StringType(),True),
    StructField("quantity",IntegerType(),True),
    StructField("unit_price",DoubleType(),True),
    StructField("run_number",LongType(),True)
])

# Bronze orders schema
order_schema = StructType([
    StructField("order_id",StringType(),True),
    StructField("order_timestamp",TimestampType(),True),
    StructField("order_status",StringType(),True),
    StructField("customer_id",StringType(),True),
    StructField("customer_name",StringType(),True),
    StructField("city",StringType(),True),
    StructField("state",StringType(),True),
    StructField("items",ArrayType(item_schema),True),
    StructField("payment_method",StringType(),True),
    StructField("payment_status",StringType(),True),
    StructField("run_number",LongType(),True)
])
# Bronze payments schema
payment_schema = StructType([
    StructField("payment_id",StringType(),True),
    StructField("order_id",StringType(),True),
    StructField("customer_id",StringType(),True),
    StructField("payment_method",StringType(),True),
    StructField("payment_status",StringType(),True),
    StructField("payment_timestamp",TimestampType(),True),
    StructField("amount",DoubleType(),True),
    StructField("transaction_id",StringType(),True),
    StructField("gateway_response_code",StringType(),True),
    StructField("retry_count",IntegerType(),True)
])

# Bronze clickstream schema
clickstream_schema = StructType([
    StructField("event_id",StringType(),True),
    StructField("session_id",StringType(),True),
    StructField("customer_id",StringType(),True),
    StructField("event_type",StringType(),True),
    StructField("product_id",StringType(),True),
    StructField("event_timestamp",TimestampType(),True),
    StructField("device",StringType(),True)
])
# Bronze Dead Letter Schema
dead_letter_schema = StructType([
    StructField("raw_message", StringType(), True),
    StructField("error", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("ingested_at", TimestampType(), True)
])