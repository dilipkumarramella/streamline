# Databricks notebook source
# ─────────────────────────────────
# IMPORTS
# ─────────────────────────────────
import sys
bundle_root = dbutils.widgets.get("bundle_root")
sys.path.append(bundle_root)

from resources.notebooks.utils.config import get_config

from resources.notebooks.utils.schema_def import (
    order_schema,
    payment_schema,
    clickstream_schema
)

from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp
)

# COMMAND ----------

# ─────────────────────────────────
# CONFIGS
# ─────────────────────────────────
env = dbutils.widgets.get("env")
config = get_config(env=env)

topic = dbutils.widgets.get("topic")


# COMMAND ----------

# ─────────────────────────────────
# REPARSE DLQ
# ─────────────────────────────────

def reprocess_dlq(topic: str):
    """
    Reprocess dead letter records for a given topic.
    """

    schema_map = {
        "orders": order_schema,
        "payments": payment_schema,
        "clickstream": clickstream_schema
    }

    table_map = {
        "orders": config["bronze_orders"],
        "payments": config["bronze_payments"],
        "clickstream": config["bronze_clickstream"]
    }

    if topic not in schema_map:
        raise ValueError(f"Invalid topic: {topic}")

    schema = schema_map[topic]
    target = table_map[topic]

    # Read DLQ records
    dlq_df = (
        spark.read.table(config["bronze_dead_letter"])
        .filter(f"topic = '{topic}'")
        .select("raw_message")
    )

    if dlq_df.count() == 0:
        print(f"No DLQ records for topic: {topic}")
        return

    # Re-parse
    reparsed_df = dlq_df.withColumn(
        "parsed",
        from_json(col("raw_message"), schema)
    ).cache()

    first_field = schema.fields[0].name

    good_df = (
        reparsed_df
        .filter(col(f"parsed.{first_field}").isNotNull())
        .select("parsed.*")
        .withColumn("ingested_at", current_timestamp())
        .dropDuplicates([first_field])
    )

    bad_df = reparsed_df.filter(col(f"parsed.{first_field}").isNull())

    good_count = good_df.count()
    bad_count  = bad_df.count()

    if good_count > 0:
        good_df.write.format("delta").mode("append").saveAsTable(target)
        print(f"Reprocessed {good_count} records to {target}")

    if bad_count > 0:
        print(f"{bad_count} records still unparseable — investigate raw_message")

# COMMAND ----------

# ─────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────

reprocess_dlq(topic)
