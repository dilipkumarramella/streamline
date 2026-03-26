# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# ─────────────────────────────────
# IMPORTS
# ─────────────────────────────────
import sys
bundle_root = dbutils.widgets.get("bundle_root")
sys.path.append(bundle_root)

 
from resources.notebooks.utils.config import get_config
from resources.notebooks.utils.delta_helpers import (
    write_data,
    table_exists,
    enable_cdf,
    get_table_location,
    column_exists
)
from resources.notebooks.utils.schema_def import (
    item_schema,
    order_schema,
    payment_schema,
    clickstream_schema
)
 
from faker import Faker
import random
import uuid
from datetime import datetime, timedelta
import time
 
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

# ─────────────────────────────────
# CONFIGS
# ─────────────────────────────────
# env = dbutils.widgets.get("env")
config = get_config(env="dev")
fake = Faker('en_IN')

dbutils.widgets.text("sleep_seconds", "1")
SLEEP_SECONDS = int(dbutils.widgets.get("sleep_seconds"))

# COMMAND ----------

# ─────────────────────────────────
# CONSTANTS
# ─────────────────────────────────
BAD_DATA_PERCENTAGE = round(random.uniform(0, 0.075), 3)

# Unique run identifier
# Increases every run!
RUN_NUMBER = int(time.time())

ORDER_STATUSES = ["delivered", "pending", "cancelled", "returned"]
PAYMENT_METHODS = ["upi", "card", "cod", "netbanking"]
PAYMENT_STATUSES = ["success", "failed", "pending"]
CATEGORIES = ["electronics", "clothing", "food", "books", "sports"]
INVALID_ORDER_STATUSES = ["unknown", "processing", "invalid", "null"]
EVENT_TYPES = ["view", "add_to_cart", "checkout", "purchase"]
DEVICES = ["mobile", "desktop", "tablet", "unknown"]

# Fixed pools for realistic data
# Same customers and products
# appear across multiple orders!
CUSTOMER_POOL_SIZE = 400
PRODUCT_POOL_SIZE = 150

# COMMAND ----------

# ─────────────────────────────────
# TIMESTAMP HELPER
# ─────────────────────────────────
 
def get_next_timestamp():
    """
    Generate sequential timestamp for this run.
    Uses yesterday's date with current time.
    Since job runs every 30-60 seconds via
    Databricks Jobs, datetime.now() naturally
    increments each run - no manual offset needed.
    Result: sequential timestamps across all runs.
    No future timestamps - safe for DLT watermarking.
    """
    return datetime.now() - timedelta(days=1)

# COMMAND ----------

# ─────────────────────────────────
# POOL FUNCTIONS
# ─────────────────────────────────

def get_or_create_customer_pool():
    """
    Get existing customer pool from Delta
    or create new one if not exists.
    Pool is STATIC - never changes!
    Same customers across all runs!
    SCD2 testing handled via
    changed_customer_location bad type
    in generate_order()!
    """
    if table_exists(spark, config["customer_pool"]):
        pool = [
            row.asDict()
            for row in spark.table(
                config["customer_pool"]
            ).collect()
        ]
        return pool
 
    else:
        pool = [
            {
                "customer_id": str(uuid.uuid4()),
                "customer_name": fake.name().lower(),
                "city": fake.city().lower(),
                "state": fake.state().lower()
            }
            for _ in range(CUSTOMER_POOL_SIZE)
        ]
 
        pool_df = spark.createDataFrame(pool)
        write_data(
            spark=spark,
            df=pool_df,
            file_type="delta",
            mode="overwrite",
            table_name=config["customer_pool"],
            location=get_table_location(
                config["bronze_path"],
                config["customer_pool"]
            )
        )
 
        return pool


def get_or_create_product_pool():
    """
    Get existing product pool from Delta
    or create new one if not exists.
    Products dont change over time!
    Same product catalog across runs!
    """
    if table_exists(spark, config["product_pool"]):
        pool = [
            row.asDict()
            for row in spark.table(
                config["product_pool"]
            ).collect()
        ]
        return pool

    else:
        pool = [
            {
                "product_id": str(uuid.uuid4()),
                "product_name": fake.word().lower(),
                "category": random.choice(CATEGORIES)
            }
            for _ in range(PRODUCT_POOL_SIZE)
        ]

        pool_df = spark.createDataFrame(pool)
        write_data(
            spark=spark,
            df=pool_df,
            file_type="delta",
            mode="overwrite",
            table_name=config["product_pool"],
            location=get_table_location(
                config["bronze_path"],
                config["product_pool"]
            )
        )
        return pool

# COMMAND ----------

# ────────────────────────────────────────────
# HELPER FUNCTIONS FOR GENERATING ORDERS DATA
# ────────────────────────────────────────────

def generate_items(product_pool, is_bad=False, change_product=False):
    """Generate list of order items from product pool."""
    num_items = random.randint(1, 5)
    items = []
    for _ in range(num_items):
        product = random.choice(product_pool)
        items.append({
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "category": random.choice(CATEGORIES) if change_product else product["category"],
            "quantity": random.randint(1, 10) if not is_bad else random.choice([0, -1]),
            "unit_price": round(random.uniform(10, 5000), 2) if not is_bad else round(random.uniform(-500, -10), 2),
            "run_number": RUN_NUMBER + 1 if change_product else RUN_NUMBER
        })
    return items

# COMMAND ----------

# ─────────────────────────────────
# GENERATE ORDERS DATA
# ─────────────────────────────────

def generate_order(customer_pool, product_pool):
    """
    Generate one order record.
    Randomly injects bad data based on
    BAD_DATA_PERCENTAGE for this run.
    Sequential timestamp via get_next_timestamp().
    """
    customer  = random.choice(customer_pool)
    timestamp = get_next_timestamp()
 
    order = {
        "order_id":        str(uuid.uuid4()),
        "order_timestamp": timestamp,
        "order_status":    random.choice(ORDER_STATUSES),
        "customer_id":     customer["customer_id"],
        "customer_name":   customer["customer_name"],
        "city":            customer["city"],
        "state":           customer["state"],
        "items":           generate_items(product_pool, is_bad=False),
        "payment_method":  random.choice(PAYMENT_METHODS),
        "payment_status":  random.choice(PAYMENT_STATUSES),
        "run_number":      RUN_NUMBER
    }
 
    # Randomly inject bad data
    if random.random() < BAD_DATA_PERCENTAGE:
        bad_types = [
            "null_customer_id",
            "null_customer_id",
            "null_customer_id",
            "negative_price",
            "negative_price",
            "negative_price",
            "future_timestamp",
            "future_timestamp",
            "invalid_status",
            "invalid_status",
            "invalid_status",
            "changed_customer_location",
            "changed_customer_location",
            "changed_customer_location",
            "changed_product_category",
            "changed_product_category",
            "changed_product_category",
        ]
        bad_type = random.choice(bad_types)
        order = inject_bad_order(order, bad_type, product_pool)
 
    return order
 
 
def inject_bad_order(order, bad_type, product_pool):
    """Inject specific bad data into order."""
 
    if bad_type == "null_customer_id":
        order["customer_id"] = None
 
    elif bad_type == "negative_price":
        order["items"] = generate_items(product_pool, is_bad=True)
 
    elif bad_type == "future_timestamp":
        order["order_timestamp"] = datetime.now() + timedelta(
            days=random.randint(1, 30)
        )
 
    elif bad_type == "invalid_status":
        order["order_status"] = random.choice(INVALID_ORDER_STATUSES)
 
    elif bad_type == "changed_customer_location":
        order["city"]       = fake.city().lower()
        order["state"]      = fake.state().lower()
        order["run_number"] = RUN_NUMBER + 1
 
    elif bad_type == "changed_product_category":
        order["items"] = generate_items(
            product_pool,
            change_product=True
        )
 
    return order

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO ORDERS BRONZE
# ─────────────────────────────────
def write_to_bronze_orders(orders: list):
    """
    Convert orders list to DataFrame and
    write to bronze.orders table.
    Adds ingested_at audit column.
    Enables CDF if not already enabled.
    """

    df = spark.createDataFrame(orders, schema=order_schema)

    # Add ingested_at
    df = df.withColumn("ingested_at", current_timestamp())

    write_data(
        spark=spark,
        df=df,
        file_type="delta",
        table_name=config["bronze_orders"],
        location=get_table_location(
            config["bronze_path"],
            config["bronze_orders"]
        )
    )

    # Enable CDF only if not already enabled
    enable_cdf(spark, config["bronze_orders"])

    print(f"Order written to {config['bronze_orders']} | order_id: {orders[0]['order_id']}")

# COMMAND ----------

# ─────────────────────────────────
# GENERATE PAYMENTS DATA
# ─────────────────────────────────

def generate_payment(order: dict):
    """
    Generate one payment record linked to order.
    Accepts order dict directly for referential
    integrity - no bronze read needed!
    order_id and customer_id sourced from
    the same order generated in this iteration.
    Randomly injects bad data based on
    BAD_DATA_PERCENTAGE for this run.
    Sequential timestamp via get_next_timestamp().
    Weighted payment_status - realistic distribution:
    80% success, 12% failed, 8% pending.
    """
    timestamp    = get_next_timestamp()
 
    payment = {
        "payment_id":            str(uuid.uuid4()),
        "order_id":              order["order_id"],
        "customer_id":           order["customer_id"],
        "payment_method":        random.choice(PAYMENT_METHODS),
        "payment_status":        random.choices(
                                     PAYMENT_STATUSES,
                                     weights=[80, 12, 8],
                                     k=1
                                 )[0],
        "payment_timestamp":     timestamp,
        "amount":                round(random.uniform(10, 5000), 2),
        "transaction_id":        str(uuid.uuid4()),
        "gateway_response_code": random.choice(["00", "01", "02", "05"]),
        "retry_count":           random.randint(0, 3)
    }
 
    # Randomly inject bad data
    if random.random() < BAD_DATA_PERCENTAGE:
        bad_types = [
            "null_payment_id",
            "null_payment_id",
            "null_order_id",
            "null_order_id",
            "negative_amount",
            "negative_amount",
            "negative_amount",
            "invalid_payment_status",
            "invalid_payment_status",
            "invalid_payment_status",
            "future_timestamp",
            "future_timestamp",
        ]
        bad_type = random.choice(bad_types)
        payment  = inject_bad_payment(payment, bad_type)
 
    return payment

def inject_bad_payment(payment, bad_type):
    """
    Inject specific bad data into payment.
    Called only when random() < BAD_DATA_PERCENTAGE.
    """
    if bad_type == "null_payment_id":
        payment["payment_id"] = None
 
    elif bad_type == "null_order_id":
        payment["order_id"] = None
 
    elif bad_type == "negative_amount":
        payment["amount"] = round(random.uniform(-500, -10), 2)
 
    elif bad_type == "invalid_payment_status":
        payment["payment_status"] = random.choice(
            ["unknown", "processing", "invalid"]
        )
 
    elif bad_type == "future_timestamp":
        payment["payment_timestamp"] = datetime.now() + timedelta(
            days=random.randint(1, 30)
        )
 
    return payment

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO PAYMENTS BRONZE
# ─────────────────────────────────

def write_to_bronze_payments(payments: list):
    """Write payments to bronze.payments table."""

    df = spark.createDataFrame(payments, schema=payment_schema)
    df = df.withColumn("ingested_at", current_timestamp())

    write_data(
        spark=spark,
        df=df,
        file_type="delta",
        table_name=config["bronze_payments"],
        location=get_table_location(
            config["bronze_path"],
            config["bronze_payments"]
        )
    )

    # Enable CDF only if not already enabled
    enable_cdf(spark, config["bronze_payments"])

    print(f"Payment written to {config['bronze_payments']} | payment_id: {payments[0]['payment_id']}")


# COMMAND ----------

# ─────────────────────────────────
# GENERATE CLICKSTREAM DATA
# ─────────────────────────────────

def generate_event(customer_pool, product_pool):
    """
    Generate one clickstream event record.
    Reads from customer and product pools directly.
    Clickstream is independent browsing behaviour
    not tied to placed orders - a customer can view
    products they never order.
    Randomly injects bad data based on
    BAD_DATA_PERCENTAGE for this run.
    Sequential timestamp via get_next_timestamp().
    Weighted event_type - realistic funnel distribution:
    55% view, 25% add_to_cart, 12% checkout, 8% purchase.
    """
    customer  = random.choice(customer_pool)
    product   = random.choice(product_pool)
    timestamp = get_next_timestamp()
 
    event = {
        "event_id":        str(uuid.uuid4()),
        "session_id":      str(uuid.uuid4()),
        "customer_id":     customer["customer_id"],
        "event_type":      random.choices(
                               EVENT_TYPES,
                               weights=[55, 25, 12, 8],
                               k=1
                           )[0],
        "product_id":      product["product_id"],
        "event_timestamp": timestamp,
        "device":          random.choice(DEVICES)
    }
 
    # Randomly inject bad data
    if random.random() < BAD_DATA_PERCENTAGE:
        bad_types = [
            "null_event_id",
            "null_event_id",
            "null_event_id",
            "null_customer_id",
            "null_customer_id",
            "null_customer_id",
            "invalid_event_type",
            "invalid_event_type",
            "invalid_event_type",
            "future_timestamp",
            "future_timestamp",
        ]
        bad_type = random.choice(bad_types)
        event    = inject_bad_event(event, bad_type)
 
    return event
 
 
def inject_bad_event(event, bad_type):
    """Inject specific bad data into event."""
 
    if bad_type == "null_event_id":
        event["event_id"] = None
 
    elif bad_type == "null_customer_id":
        event["customer_id"] = None
 
    elif bad_type == "invalid_event_type":
        event["event_type"] = random.choice(
            ["unknown", "click", "invalid"]
        )
 
    elif bad_type == "future_timestamp":
        event["event_timestamp"] = datetime.now() + timedelta(
            days=random.randint(1, 30)
        )
 
    return event

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO CLICKSTREAM BRONZE
# ─────────────────────────────────

def write_to_bronze_clickstream(events: list):
    """Write clickstream events to bronze.clickstream table."""

    df = spark.createDataFrame(events, schema=clickstream_schema)
    df = df.withColumn("ingested_at", current_timestamp())

    write_data(
        spark=spark,
        df=df,
        file_type="delta",
        table_name=config["bronze_clickstream"],
        location=get_table_location(
            config["bronze_path"],
            config["bronze_clickstream"]
        )
    )

    # Enable CDF only if not already enabled
    enable_cdf(spark, config["bronze_clickstream"])

    print(f"Event written to {config['bronze_clickstream']} | event_id: {events[0]['event_id']}")

# COMMAND ----------

# ─────────────────────────────────
# MAIN
# ─────────────────────────────────
 
def run_pipeline():
    """
    Main pipeline function.
    Runs continuously until job is manually stopped.
    Pools loaded once at start - not every iteration!
    No optimize - vacuum job handles maintenance.
 
    Flow:
    1. Load customer and product pools (once)
    2. Loop forever every SLEEP_SECONDS:
       a. Generate 1 order
       b. Generate 1 payment linked to same order dict
          (referential integrity without bronze read)
       c. Generate 1 clickstream event from pools
       d. Write all 3 to bronze
       e. Sleep SLEEP_SECONDS
 
    Widget:
        sleep_seconds: Seconds to sleep between runs
                       Default: 30
                       Lower = more data = more credits!
    """
    customer_pool = get_or_create_customer_pool()
    product_pool  = get_or_create_product_pool()
 
    run_count = 0
    while True:
        order   = generate_order(customer_pool, product_pool)
        payment = generate_payment(order)
        event   = generate_event(customer_pool, product_pool)
 
        write_to_bronze_orders([order])
        write_to_bronze_payments([payment])
        write_to_bronze_clickstream([event])
 
        run_count += 1
        print(f"Run {run_count} complete — sleeping {SLEEP_SECONDS}s")
        time.sleep(SLEEP_SECONDS)

# COMMAND ----------

# ─────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────
run_pipeline()
