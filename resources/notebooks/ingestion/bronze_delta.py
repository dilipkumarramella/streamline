# Databricks notebook source
# ─────────────────────────────────
# IMPORTS
# ─────────────────────────────────
import sys, time

bundle_root = dbutils.widgets.get("bundle_root")
sys.path.append(bundle_root)
 
from resources.notebooks.utils.config import get_config
from resources.notebooks.utils.delta_helpers import (
    read_stream_data,
    write_stream_data,
    table_exists,
    enable_cdf
)
from resources.notebooks.utils.schema_def import (
    order_schema,
    payment_schema,
    clickstream_schema
)
 
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)

# COMMAND ----------

# ─────────────────────────────────
# CONFIGS
# ─────────────────────────────────
env = dbutils.widgets.get("env")
config = get_config(env=env)

# COMMAND ----------

# ─────────────────────────────────
# KAFKA CONFIG
# ─────────────────────────────────
 
# Shared Kafka options for all topics
KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": config["KAFKA_BOOTSTRAP_SERVERS"],
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism":    "PLAIN",
    "kafka.sasl.jaas.config":  f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{config["KAFKA_API_KEY"]}" password="{config["KAFKA_API_SECRET"]}";',
    "startingOffsets":         "latest",
    "failOnDataLoss":          "false"
}

# COMMAND ----------

# ─────────────────────────────────
# PARSE WITH DLQ
# ─────────────────────────────────
 
def parse_with_dlq(raw_df, schema, topic: str):
    """
    Parse raw Kafka binary messages into structured DataFrame.
    Unparseable messages go to DLQ — stream never crashes.
 
    Strategy:
    - Cast binary value to string
    - Attempt from_json with expected schema
    - Records where first schema field is null = parse failure
    - Parse failures → dead_letter table
    - Successfully parsed → bronze table
 
    Args:
        raw_df: Raw Kafka DataFrame (binary value column)
        schema: StructType schema to parse JSON into
        topic:  Kafka topic name (stored in DLQ for tracing)
 
    Returns:
        tuple: (good_df, dlq_df)
               good_df = successfully parsed + ingested_at added
               dlq_df  = unparseable records with error info
    """
    string_df = raw_df.selectExpr("CAST(value AS STRING) AS raw_message")
 
    parsed_df = string_df.withColumn(
        "parsed",
        from_json(col("raw_message"), schema)
    )
 
    # Use first schema field as parse success indicator
    first_field = schema.fields[0].name
 
    good_df = (
        parsed_df
        .filter(col(f"parsed.{first_field}").isNotNull())
        .select("parsed.*")
        .withColumn("ingested_at", current_timestamp())
    )
 
    dlq_df = (
        parsed_df
        .filter(col(f"parsed.{first_field}").isNull())
        .select(
            col("raw_message"),
            lit(f"Failed to parse JSON from topic: {topic}").alias("error"),
            lit(topic).alias("topic"),
            current_timestamp().alias("ingested_at")
        )
    )
 
    return good_df, dlq_df

# COMMAND ----------

# ─────────────────────────────────
# ENABLE CDF — POST FIRST WRITE
# ─────────────────────────────────
 
def enable_bronze_cdf():
    """
    Enable CDF on all bronze tables after
    first micro-batch creates them.
    Silver pipelines require CDF on bronze.
    enable_cdf() is idempotent — safe to
    call multiple times.
    """
    for table in [
        config["bronze_orders"],
        config["bronze_payments"],
        config["bronze_clickstream"],
        config["bronze_dead_letter"]
    ]:
        
        retries = 3
        success = False

    while retries > 0:
        if table_exists(spark, table):
            enable_cdf(spark, table)
            success = True
            break
        else:
            print(f"{table} not found, retrying in 30s...")
            time.sleep(30)
            retries -= 1

    if not success:
            print(f"Table not yet created: {table} — retry manually")

# COMMAND ----------

# ─────────────────────────────────
# PIPELINE
# ─────────────────────────────────
 
def run_pipeline():
    """
    Main streaming pipeline function.
    Reads 3 Kafka topics, parses JSON,
    routes good records to bronze tables
    and unparseable records to dead_letter.
    All 6 streams run concurrently.
    CDF enabled after first micro-batch.
 
    Flow per topic:
    1. read_stream_data() — Kafka source
    2. parse_with_dlq()   — JSON parse + DLQ split
    3. write_stream_data() — good → bronze Delta
    4. write_stream_data() — bad  → bronze.dead_letter
    5. enable_bronze_cdf() — after first batch
    6. awaitAnyTermination — run until job stopped
 
    Streams run 24/7 until job is manually stopped.
    failOnDataLoss=false handles Kafka offset gaps.
    """
    print("Starting bronze streaming pipeline...")
 
    # Orders ──────────────────────────────
    raw_orders = read_stream_data(
        spark=spark,
        file_type="kafka",
        options={**KAFKA_OPTIONS, "subscribe": config["KAFKA_TOPIC_ORDERS"]}
    )
 
    good_orders, dlq_orders = parse_with_dlq(
        raw_df=raw_orders,
        schema=order_schema,
        topic=config["KAFKA_TOPIC_ORDERS"]
    )
 
    write_stream_data(
        df=good_orders,
        file_type="delta",
        checkpoint_location=config["checkpoint_path"] + "bronze_orders/",
        table_name=config["bronze_orders"]
    )
 
    write_stream_data(
        df=dlq_orders,
        file_type="delta",
        checkpoint_location=config["checkpoint_path"] + "bronze_orders_dlq/",
        table_name=config["bronze_dead_letter"]
    )
 
    # Payments ────────────────────────────
    raw_payments = read_stream_data(
        spark=spark,
        file_type="kafka",
        options={**KAFKA_OPTIONS, "subscribe": config["KAFKA_TOPIC_PAYMENTS"]}
    )
 
    good_payments, dlq_payments = parse_with_dlq(
        raw_df=raw_payments,
        schema=payment_schema,
        topic=config["KAFKA_TOPIC_PAYMENTS"]
    )
 
    write_stream_data(
        df=good_payments,
        file_type="delta",
        checkpoint_location=config["checkpoint_path"] + "bronze_payments/",
        table_name=config["bronze_payments"]
    )
 
    write_stream_data(
        df=dlq_payments,
        file_type="delta",
        checkpoint_location=config["checkpoint_path"] + "bronze_payments_dlq/",
        table_name=config["bronze_dead_letter"]
    )
 
    # Clickstream ─────────────────────────
    raw_clickstream = read_stream_data(
        spark=spark,
        file_type="kafka",
        options={**KAFKA_OPTIONS, "subscribe": config["KAFKA_TOPIC_CLICKSTREAM"]}
    )
 
    good_clickstream, dlq_clickstream = parse_with_dlq(
        raw_df=raw_clickstream,
        schema=clickstream_schema,
        topic=config["KAFKA_TOPIC_CLICKSTREAM"]
    )
 
    write_stream_data(
        df=good_clickstream,
        file_type="delta",
        checkpoint_location=config["checkpoint_path"] + "bronze_clickstream/",
        table_name=config["bronze_clickstream"]
    )
 
    write_stream_data(
        df=dlq_clickstream,
        file_type="delta",
        checkpoint_location=config["checkpoint_path"] + "bronze_clickstream_dlq/",
        table_name=config["bronze_dead_letter"]
    )
 
    print("All 6 streams started (3 bronze + 3 DLQ)")
 
    # Enable CDF after first micro-batch
    enable_bronze_cdf()

# COMMAND ----------

# ─────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────
run_pipeline()
