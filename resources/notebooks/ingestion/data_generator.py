# Databricks notebook source
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
from kafka import KafkaProducer
import random
import uuid
from datetime import datetime, timedelta
import time
import json
 
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

# ─────────────────────────────────
# CONFIGS
# ─────────────────────────────────
env = dbutils.widgets.get("env")
config = get_config(env=env)
fake = Faker('en_IN')

dbutils.widgets.text("sleep_seconds", "10")
SLEEP_SECONDS = int(dbutils.widgets.get("sleep_seconds"))

# COMMAND ----------

# ─────────────────────────────────
# CONSTANTS
# ─────────────────────────────────
BAD_DATA_PERCENTAGE = round(random.uniform(0, 0.075), 3)

# Unique run identifier, increases every run
RUN_NUMBER = int(time.time())

ORDER_STATUSES = ["delivered", "pending", "cancelled", "returned"]
PAYMENT_METHODS = ["upi", "card", "cod", "netbanking"]
PAYMENT_STATUSES = ["success", "failed", "pending"]
CATEGORIES = ["electronics", "clothing", "food", "books", "sports"]
INVALID_ORDER_STATUSES = ["unknown", "processing", "invalid", "null"]
EVENT_TYPES = ["view", "add_to_cart", "checkout", "purchase"]
DEVICES = ["mobile", "desktop", "tablet", "unknown"]

# Fixed pools for realistic data, Same customers and products appear across multiple orders(Compliments SCD2)
CUSTOMER_POOL_SIZE = 400
PRODUCT_POOL_SIZE = 150

# COMMAND ----------

# ─────────────────────────────────
# KAFKA PRODUCER
# ─────────────────────────────────
 
def get_kafka_producer():
    """
    Create and return a Kafka producer.
    Uses SASL_SSL for Confluent Cloud auth.
    JSON serializer — Schema Registry in future.
 
    Returns:
        KafkaProducer instance
    """
    return KafkaProducer(
        bootstrap_servers=config["KAFKA_BOOTSTRAP_SERVERS"],
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=config["KAFKA_API_KEY"],
        sasl_plain_password=config["KAFKA_API_SECRET"],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=3
    )

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
# PRODUCE TO KAFKA — ORDERS
# ─────────────────────────────────
 
def produce_order(producer, order: dict):
    """
    Produce one order record to Kafka orders topic.
    Uses order_id as message key for partitioning.
    Timestamps serialized to string via json default=str.
    """
    try:
        producer.send(
            config["KAFKA_TOPIC_ORDERS"],
            key=str(order["order_id"]).encode("utf-8"),
            value=order
        )
        print(f"Order produced to {config["KAFKA_TOPIC_ORDERS"]} | order_id: {order['order_id']}")
    
    except Exception as e:
        print(f"Kafka send failed for order_id {order['order_id']}: {e}")

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
# PRODUCE TO KAFKA — PAYMENTS
# ─────────────────────────────────
 
def produce_payment(producer, payment: dict):
    """
    Produce one payment record to Kafka payments topic.
    Uses payment_id as message key for partitioning.
    """
    try:
        producer.send(
            config["KAFKA_TOPIC_PAYMENTS"],
            key=str(payment["payment_id"]).encode("utf-8"),
            value=payment
        )
        print(f"Payment produced to {config["KAFKA_TOPIC_PAYMENTS"]} | payment_id: {payment['payment_id']}")
    
    except Exception as e:
        print(f"Kafka send failed for payment_id {payment['payment_id']}: {e}")

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
# PRODUCE TO KAFKA — CLICKSTREAM
# ─────────────────────────────────
 
def produce_event(producer, event: dict):
    """
    Produce one clickstream event to Kafka clickstream topic.
    Uses event_id as message key for partitioning.
    """
    try:
        producer.send(
            config["KAFKA_TOPIC_CLICKSTREAM"],
            key=str(event["event_id"]).encode("utf-8"),
            value=event
        )
        print(f"Event produced to {config["KAFKA_TOPIC_CLICKSTREAM"]} | event_id: {event['event_id']}")

    except Exception as e:
        print(f"Kafka send failed for event_id {event['order_id']}: {e}")

# COMMAND ----------

# ─────────────────────────────────
# MAIN
# ─────────────────────────────────
 
def run_pipeline():
    """
    Main pipeline function.
    Runs continuously until job is manually stopped.
    Pools loaded once at start - not every iteration!
    Producer created once - reused across all iterations.
    Producer closed cleanly on job stop via finally block.
 
    Flow:
    1. Load customer and product pools (once)
    2. Create Kafka producer (once)
    3. Loop forever every SLEEP_SECONDS:
       a. Generate 1 order
       b. Generate 1 payment linked to same order dict
          (referential integrity without bronze read)
       c. Generate 1 clickstream event from pools
       d. Produce all 3 to Kafka topics
       e. Flush producer to ensure delivery
       f. Sleep SLEEP_SECONDS
 
    Widget:
        sleep_seconds: Seconds to sleep between runs
                       Default: 30
                       Lower = more data = more credits!
    """
    customer_pool = get_or_create_customer_pool()
    product_pool  = get_or_create_product_pool()
    producer      = get_kafka_producer()
 
    run_count = 0
    try:
        while True:
            order   = generate_order(customer_pool, product_pool)
            payment = generate_payment(order)
            event   = generate_event(customer_pool, product_pool)
 
            produce_order(producer, order)
            produce_payment(producer, payment)
            produce_event(producer, event)
 
            # Flush ensures all messages delivered
            producer.flush()
 
            run_count += 1
            print(f"Run {run_count} complete — sleeping {SLEEP_SECONDS}s")
            time.sleep(SLEEP_SECONDS)
 
    finally:
        # Always closes producer cleanly even if job is manually stopped
        producer.close()
        print("Kafka producer closed")

# COMMAND ----------

# ─────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────
run_pipeline()
