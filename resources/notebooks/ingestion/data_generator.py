# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# ─────────────────────────────────
# IMPORTS
# ─────────────────────────────────
import sys
sys.path.append('/Workspace/Users/dilip.dot.dot@gmail.com/streamline/')

from resources.notebooks.utils.config import get_config
from resources.notebooks.utils.delta_helpers import write_data, table_exists, enable_cdf, get_table_location,column_exists

from faker import Faker
import random
import uuid
from datetime import datetime, timedelta
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, explode

# COMMAND ----------

# ─────────────────────────────────
# CONFIGS
# ─────────────────────────────────
#env = dbutils.widgets.get("env")
config = get_config(env="dev")
fake = Faker('en_IN')

# COMMAND ----------

# ─────────────────────────────────
# CONSTANTS
# ─────────────────────────────────
TOTAL_RECORDS = 1000
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

print(f"Bad data percentage this run: {BAD_DATA_PERCENTAGE * 100:.1f}%")
print(f"Run number: {RUN_NUMBER}")

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
    in generate_orders()!
    """
    if table_exists(spark, config["customer_pool"]):
        print("Loading existing customer pool...")
        pool = [
            row.asDict()
            for row in spark.table(
                config["customer_pool"]
            ).collect()
        ]
        print(f"Loaded {len(pool)} customers")
        return pool

    else:
        print("Creating new customer pool...")
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

        print(f"Customer pool created with {CUSTOMER_POOL_SIZE} customers")
        return pool


def get_or_create_product_pool():
    """
    Get existing product pool from Delta
    or create new one if not exists.
    Products dont change over time!
    Same product catalog across runs!
    """
    if table_exists(spark, config["product_pool"]):
        print("Loading existing product pool...")
        pool = [
            row.asDict()
            for row in spark.table(
                config["product_pool"]
            ).collect()
        ]
        print(f"Loaded {len(pool)} products")
        return pool

    else:
        print("Creating new product pool...")
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

        print(f"Product pool created with {PRODUCT_POOL_SIZE} products")
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


def generate_good_order(customer_pool, product_pool):
    """Generate one valid order record from customer pool."""
    customer = random.choice(customer_pool)

    return {
        "order_id": str(uuid.uuid4()),
        "order_timestamp": fake.date_time_between(
            start_date="-1d",
            end_date="now"
        ),
        "order_status": random.choice(ORDER_STATUSES),
        "customer_id": customer["customer_id"],
        "customer_name": customer["customer_name"],
        "city": customer["city"],
        "state": customer["state"],
        "items": generate_items(product_pool, is_bad=False),
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": random.choice(PAYMENT_STATUSES),
        "run_number": RUN_NUMBER
    }


def inject_bad_data(order, bad_type, product_pool):
    """Inject specific bad data into order"""

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
        order["city"] = fake.city().lower()
        order["state"] = fake.state().lower()
        order["run_number"] = RUN_NUMBER + 1
    
    elif bad_type == "changed_product_category":
        order["items"] = generate_items(
        product_pool,
        change_product=True
    )

    elif bad_type == "duplicate_order_id":
        pass

    return order

# COMMAND ----------

# ─────────────────────────────────
# GENERATE ORDERS DATA
# ─────────────────────────────────

def generate_orders(customer_pool, product_pool):
    """Generate orders with random bad data percentage."""

    orders = []
    bad_count = int(TOTAL_RECORDS * BAD_DATA_PERCENTAGE)
    good_count = TOTAL_RECORDS - bad_count

    print(f"Generating {good_count} good orders and {bad_count} bad orders")

    # Generate good records
    for _ in range(good_count):
        orders.append(generate_good_order(customer_pool, product_pool))

    # Generate bad records
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
        "duplicate_order_id",
        "duplicate_order_id",
        "changed_customer_location",
        "changed_customer_location",
        "changed_customer_location",
        "changed_product_category",
        "changed_product_category",
        "changed_product_category",
    ]

    # Pick random bad types for bad records
    for i in range(bad_count):
        order = generate_good_order(customer_pool, product_pool)
        bad_type = bad_types[i % len(bad_types)]
        order = inject_bad_data(order, bad_type, product_pool)
        orders.append(order)

    # Inject duplicate order ids
    duplicate_ids = [orders[i]["order_id"] for i in range(10)]
    for i, order in enumerate(orders):
        if order.get("order_id") in duplicate_ids and i > 10:
            order["order_id"] = random.choice(duplicate_ids)

    # Shuffle so bad data is mixed in
    random.shuffle(orders)

    return orders

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO ORDERS BRONZE
# ─────────────────────────────────
def write_to_bronze_orders(orders):
    """Convert orders to DataFrame and write to bronze.orders"""

    df = spark.createDataFrame(orders)

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

    print(f"Written {df.count()} records to {config['bronze_orders']}")

# COMMAND ----------

# ────────────────────────────────────────────
# HELPER FUNCTIONS FOR GENERATING PAYMENTS DATA
# ────────────────────────────────────────────

def get_ids_from_bronze():
    """
    Return order_id and customer_id
    from bronze for payments referential integrity.
    """
    return spark.table(
        config["bronze_orders"]
    ).select(
        "order_id",
        "customer_id"
    ).distinct().collect()


def generate_good_payment(orders):
    """Generate one valid payment record."""
    random_order = random.choice(orders)
    
    return {
        "payment_id": str(uuid.uuid4()),
        "order_id": random_order.order_id,
        "customer_id": random_order.customer_id,
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": random.choice(PAYMENT_STATUSES),
        "payment_timestamp": fake.date_time_between(
            start_date="-1d",
            end_date="now"
        ),
        "amount": round(random.uniform(10, 5000), 2),
        "transaction_id": str(uuid.uuid4()),
        "gateway_response_code": random.choice(
            ["00", "01", "02", "05"]
        ),
        "retry_count": random.randint(0, 3)
    }


def inject_bad_payment(payment, bad_type):
    """Inject specific bad data into payment."""

    if bad_type == "null_payment_id":
        payment["payment_id"] = None

    elif bad_type == "null_order_id":
        payment["order_id"] = None

    elif bad_type == "negative_amount":
        payment["amount"] = round(
            random.uniform(-500, -10), 2
        )

    elif bad_type == "invalid_payment_status":
        payment["payment_status"] = random.choice(
            ["unknown", "processing", "invalid"]
        )

    elif bad_type == "future_timestamp":
        payment["payment_timestamp"] = datetime.now() + timedelta(
            days=random.randint(1, 30)
        )

    elif bad_type == "duplicate_payment_id":
        pass  # handled separately

    return payment

# COMMAND ----------

# ─────────────────────────────────
# GENERATE PAYMENTS DATA
# ─────────────────────────────────

def generate_payments(orders):
    """Generate payments with random bad data percentage."""

    payments = []
    bad_count = int(TOTAL_RECORDS * BAD_DATA_PERCENTAGE)
    good_count = TOTAL_RECORDS - bad_count

    print(f"Generating {good_count} good payments and {bad_count} bad payments")

    # Generate good records
    for _ in range(good_count):
        payments.append(generate_good_payment(orders))

    # Bad types
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
        "duplicate_payment_id",
    ]

    # Generate bad records
    for i in range(bad_count):
        payment = generate_good_payment(orders)
        bad_type = bad_types[i % len(bad_types)]
        payment = inject_bad_payment(payment, bad_type)
        payments.append(payment)

    # Inject duplicate payment ids
    duplicate_ids = [
        payments[i]["payment_id"] for i in range(10)
    ]
    for i, payment in enumerate(payments):
        if payment.get("payment_id") in duplicate_ids and i > 10:
            payment["payment_id"] = random.choice(duplicate_ids)

    # Shuffle
    random.shuffle(payments)

    return payments

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO PAYMENTS BRONZE
# ─────────────────────────────────

def write_to_bronze_payments(payments):
    """Write payments to bronze.payments table."""

    df = spark.createDataFrame(payments)
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

    print(f"Written {df.count()} records to {config['bronze_payments']}")


# COMMAND ----------

# ────────────────────────────────────────────
# HELPER FUNCTIONS FOR GENERATING CLICKSTREAM DATA
# ────────────────────────────────────────────

def get_customer_ids_from_bronze():
    """
    Return customer_id and product_id
    from bronze for clickstream referential integrity.
    """
    from pyspark.sql.functions import explode

    return spark.table(
        config["bronze_orders"]
    ).select(
        "customer_id",
        explode("items").alias("item")
    ).select(
        "customer_id",
        col("item.product_id").alias("product_id")
    ).distinct().collect()


def generate_good_event(orders):
    """Generate one valid clickstream event record."""
    random_order = random.choice(orders)

    return {
        "event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "customer_id": random_order.customer_id,
        "event_type": random.choice(EVENT_TYPES),
        "product_id": random_order.product_id,
        "event_timestamp": fake.date_time_between(
            start_date="-1d",
            end_date="now"
        ),
        "device": random.choice(DEVICES)
    }


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

    elif bad_type == "duplicate_event_id":
        pass  # handled separately

    return event

# COMMAND ----------

# ─────────────────────────────────
# GENERATE CLICKSTREAM DATA
# ─────────────────────────────────

def generate_clickstream(orders):
    """Generate clickstream events with random bad data percentage."""

    events = []
    bad_count = int(TOTAL_RECORDS * BAD_DATA_PERCENTAGE)
    good_count = TOTAL_RECORDS - bad_count

    print(f"Generating {good_count} good events and {bad_count} bad events")

    # Generate good records
    for _ in range(good_count):
        events.append(generate_good_event(orders))

    # Bad types
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
        "duplicate_event_id",
        "duplicate_event_id",
    ]

    # Generate bad records
    for i in range(bad_count):
        event = generate_good_event(orders)
        bad_type = bad_types[i % len(bad_types)]
        event = inject_bad_event(event, bad_type)
        events.append(event)

    # Inject duplicate event ids
    duplicate_ids = [
        events[i]["event_id"] for i in range(10)
    ]
    for i, event in enumerate(events):
        if event.get("event_id") in duplicate_ids and i > 10:
            event["event_id"] = random.choice(duplicate_ids)

    # Shuffle
    random.shuffle(events)

    return events

# COMMAND ----------

# ─────────────────────────────────
# WRITE TO CLICKSTREAM BRONZE
# ─────────────────────────────────

def write_to_bronze_clickstream(events):
    """Write clickstream events to bronze.clickstream table."""

    df = spark.createDataFrame(events)
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

    print(f"Written {df.count()} records to {config['bronze_clickstream']}")

# COMMAND ----------

# ─────────────────────────────────
# MAIN
# ─────────────────────────────────
def run():
    print("Starting data generator...")
    print(f"Bad data percentage this run: {BAD_DATA_PERCENTAGE * 100:.1f}%")

    # Get or create pools
    customer_pool = get_or_create_customer_pool()
    product_pool = get_or_create_product_pool()

    # Orders
    orders = generate_orders(customer_pool, product_pool)
    print(f"Generated {len(orders)} orders")
    write_to_bronze_orders(orders)

    # Payments
    print("Fetching order ids from bronze...")
    bronze_orders = get_ids_from_bronze()
    payments = generate_payments(bronze_orders)
    print(f"Generated {len(payments)} payments")
    write_to_bronze_payments(payments)

    # Clickstream
    print("Fetching customer and product ids from bronze...")
    bronze_ids = get_customer_ids_from_bronze()
    events = generate_clickstream(bronze_ids)
    print(f"Generated {len(events)} events")
    write_to_bronze_clickstream(events)

    print("Data generator complete!")

run()
