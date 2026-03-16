# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# ─────────────────────────────────
# IMPORTS
# ─────────────────────────────────
import sys
sys.path.append('/Workspace/Users/dilip.dot.dot@gmail.com/streamline/')

from resources.notebooks.utils.config import get_config
from resources.notebooks.utils.delta_helpers import write_data, table_exists, enable_cdf

from faker import Faker
import random
import uuid
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

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
BAD_DATA_PERCENTAGE = 0.075

ORDER_STATUSES = ["delivered", "pending", "cancelled", "returned"]
PAYMENT_METHODS = ["UPI", "Card", "COD", "Netbanking"]
PAYMENT_STATUSES = ["success", "failed", "pending"]
CATEGORIES = ["Electronics", "Clothing", "Food", "Books", "Sports"]

INVALID_ORDER_STATUSES = ["unknown", "processing", "invalid", "null"]

# COMMAND ----------

# ────────────────────────────────────────────
# HELPER FUNCTIONS FOR GENERATING ORDERS DATA
# ────────────────────────────────────────────
def generate_items(is_bad=False):
    """Generate list of order items"""
    num_items = random.randint(1, 5)
    items = []
    for _ in range(num_items):
        items.append({
            "product_id": str(uuid.uuid4()),
            "product_name": fake.word().capitalize(),
            "category": random.choice(CATEGORIES),
            "quantity": random.randint(1, 10) if not is_bad else random.choice([0, -1]),
            "unit_price": round(random.uniform(10, 5000), 2) if not is_bad else round(random.uniform(-500, -10), 2)
        })
    return items


def generate_good_order():
    """Generate one valid order record"""
    return {
        "order_id": str(uuid.uuid4()),
        "order_timestamp": fake.date_time_between(
            start_date="-1d",
            end_date="now"
        ),
        "order_status": random.choice(ORDER_STATUSES),
        "customer_id": str(uuid.uuid4()),
        "customer_name": fake.name(),
        "city": fake.city(),
        "state": fake.state(),
        "items": generate_items(is_bad=False),
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": random.choice(PAYMENT_STATUSES)
    }


def inject_bad_data(order, bad_type):
    """Inject specific bad data into order"""

    if bad_type == "null_customer_id":
        order["customer_id"] = None

    elif bad_type == "negative_price":
        order["items"] = generate_items(is_bad=True)

    elif bad_type == "future_timestamp":
        order["order_timestamp"] = datetime.now() + timedelta(days=random.randint(1, 30))

    elif bad_type == "invalid_status":
        order["order_status"] = random.choice(INVALID_ORDER_STATUSES)

    elif bad_type == "duplicate_order_id":
        pass  # handled separately

    return order

# COMMAND ----------

# ─────────────────────────────────
# GENERATE ORDERS DATA
# ─────────────────────────────────
def generate_orders():
    """Generate 1000 orders with 7.5% bad data"""

    orders = []
    bad_count = int(TOTAL_RECORDS * BAD_DATA_PERCENTAGE)
    good_count = TOTAL_RECORDS - bad_count

    # Generate good records
    for _ in range(good_count):
        orders.append(generate_good_order())

    # Generate bad records
    bad_types = [
        "null_customer_id",    # 15 records
        "null_customer_id",
        "null_customer_id",
        "negative_price",      # 15 records
        "negative_price",
        "negative_price",
        "future_timestamp",    # 10 records
        "future_timestamp",
        "invalid_status",      # 15 records
        "invalid_status",
        "invalid_status",
        "duplicate_order_id",  # 10 records
        "duplicate_order_id",
    ]

    # Pick random bad types for bad records
    for i in range(bad_count):
        order = generate_good_order()
        bad_type = bad_types[i % len(bad_types)]
        order = inject_bad_data(order, bad_type)
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
def write_to_bronze(orders):
    """Convert orders to DataFrame and write to bronze.orders"""

    df = spark.createDataFrame(orders)

    # Add ingested_at
    df = df.withColumn("ingested_at", current_timestamp())

    write_data(
        df=df,
        file_type="delta",
        table_name=config["bronze_orders"]
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
    Read existing order_id and customer_id
    from bronze.orders for referential integrity.
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

def generate_payments(orders):
    """Generate 1000 payments with 7.5% bad data."""

    payments = []
    bad_count = int(TOTAL_RECORDS * BAD_DATA_PERCENTAGE)
    good_count = TOTAL_RECORDS - bad_count

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

def write_to_bronze_payments(payments):
    """Write payments to bronze.payments table."""

    df = spark.createDataFrame(payments)
    df = df.withColumn("ingested_at", current_timestamp())

    write_data(
        df=df,
        file_type="delta",
        table_name=config["bronze_payments"]
    )

    # Enable CDF only if not already enabled
    enable_cdf(spark, config["bronze_payments"])

    print(f"Written {df.count()} records to {config['bronze_payments']}")



# COMMAND ----------

# ─────────────────────────────────
# MAIN
# ─────────────────────────────────
def run():
    print("Starting data generator...")
    orders = generate_orders()
    print(f"Generated {len(orders)} orders")
    write_to_bronze(orders)
    
    # Payments
    print("Fetching order ids from bronze...")
    bronze_orders = get_ids_from_bronze()
    payments = generate_payments(bronze_orders)
    print(f"Generated {len(payments)} payments")
    write_to_bronze_payments(payments)

    print("Data generator complete!")

run()
