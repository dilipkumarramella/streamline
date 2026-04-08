import dlt
import sys
 
bundle_root = spark.conf.get("bundle_root")
sys.path.append(bundle_root)
 
from resources.notebooks.utils.config import get_config
from pyspark.sql.functions import (
    col, when, lit, count, approx_count_distinct,
    sum as spark_sum, round as spark_round,
    current_timestamp, explode
)
 
env = spark.conf.get("env", "dev")
config = get_config(env=env)
WATERMARK = "10 minutes"

@dlt.table(
    name="live_order_summary",
    comment="Live GMV and order counts by category."
)
@dlt.expect("valid_category", "category IS NOT NULL")
@dlt.expect("non_negative_revenue", "total_revenue >= 0")
def live_order_summary():
    return (
        spark.readStream
        .format("delta")
        .table(config["bronze_orders"])
        .withWatermark("order_timestamp", WATERMARK)
        .withColumn("item", explode(col("items")))
        .select(
            "order_id", "order_status", "order_timestamp",
            col("item.category").alias("category"),
            col("item.quantity").alias("quantity"),
            col("item.unit_price").alias("unit_price")
        )
        .withColumn(
            "line_revenue",
            when(col("order_status") == "delivered",
                 col("quantity") * col("unit_price")
            ).otherwise(lit(0.0))
        )
        .groupBy("category")
        .agg(
            approx_count_distinct("order_id").alias("total_orders"),
            approx_count_distinct(
                when(col("order_status") == "delivered", col("order_id"))
            ).alias("delivered_orders"),
            spark_round(spark_sum("line_revenue"), 2).alias("total_revenue")
        )
        .withColumn(
            "avg_order_value",
            when(col("delivered_orders") == 0, lit(0.0))
            .otherwise(spark_round(col("total_revenue") / col("delivered_orders"), 2))
        )
        .withColumn("created_at", current_timestamp())
    )

@dlt.table(
    name="live_payment_success_rate",
    comment="Live payment health by method."
)
@dlt.expect("valid_payment_method", "payment_method IS NOT NULL")
@dlt.expect("healthy_success_rate", "success_rate >= 0.7")
def live_payment_success_rate():
    return (
        spark.readStream
        .format("delta")
        .table(config["bronze_payments"])
        .withWatermark("payment_timestamp", WATERMARK)
        .groupBy("payment_method")
        .agg(
            count("payment_id").alias("total_attempts"),
            count(when(col("payment_status") == "success", col("payment_id"))).alias("successful"),
            count(when(col("payment_status") == "failed",  col("payment_id"))).alias("failed"),
            count(when(col("payment_status") == "pending", col("payment_id"))).alias("pending")
        )
        .withColumn(
            "success_rate",
            when(col("total_attempts") == 0, lit(0.0))
            .otherwise(spark_round(col("successful") / col("total_attempts"), 4))
        )
        .withColumn("created_at", current_timestamp())
    )

@dlt.table(
    name="live_funnel_snapshot",
    comment="Live funnel stage counts by event type."
)
@dlt.expect("valid_event_type", "event_type IS NOT NULL")
@dlt.expect("valid_event_count", "event_count > 0")
def live_funnel_snapshot():
    return (
        spark.readStream
        .format("delta")
        .table(config["bronze_clickstream"])
        .withWatermark("event_timestamp", WATERMARK)
        .groupBy("event_type")
        .agg(
            approx_count_distinct("session_id").alias("event_count")
        )
        .withColumn("created_at", current_timestamp())
    )