import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
import sys

sys.path.append('.')

# ─────────────────────────────────
# SPARK SESSION FIXTURE
# ─────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local") \
        .appName("streamline_tests") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()

# ─────────────────────────────────
# IMPORT FUNCTIONS
# ─────────────────────────────────

from resources.notebooks.utils.data_quality import (
    check_nulls,
    check_positive_values,
    check_valid_values,
    quarantine_records
)

# ─────────────────────────────────
# TEST CHECK NULLS
# ─────────────────────────────────

def test_check_nulls_flags_null_values(spark):
    """Null customer_id should be flagged."""
    data = [("order1", "cust1"), ("order2", None)]
    df = spark.createDataFrame(data, ["order_id", "customer_id"])
    result = check_nulls(df, ["customer_id"])
    bad = result.filter(col("rejection_reason").isNotNull())
    assert bad.count() == 1
    assert bad.first()["rejection_reason"] == "null_customer_id"


def test_check_nulls_passes_valid_records(spark):
    """Records with no nulls should not be flagged."""
    data = [("order1", "cust1"), ("order2", "cust2")]
    df = spark.createDataFrame(data, ["order_id", "customer_id"])
    result = check_nulls(df, ["customer_id"])
    bad = result.filter(col("rejection_reason").isNotNull())
    assert bad.count() == 0

# ─────────────────────────────────
# TEST CHECK POSITIVE VALUES
# ─────────────────────────────────

def test_check_positive_values_flags_negatives(spark):
    """Negative and zero unit_price should be flagged."""
    data = [(1, 100.0), (2, -50.0), (3, 0.0)]
    df = spark.createDataFrame(data, ["id", "unit_price"])
    result = check_positive_values(df, ["unit_price"])
    bad = result.filter(col("rejection_reason").isNotNull())
    assert bad.count() == 2


def test_check_positive_values_passes_valid_records(spark):
    """Positive unit_price should not be flagged."""
    data = [(1, 100.0), (2, 50.0)]
    df = spark.createDataFrame(data, ["id", "unit_price"])
    result = check_positive_values(df, ["unit_price"])
    bad = result.filter(col("rejection_reason").isNotNull())
    assert bad.count() == 0

# ─────────────────────────────────
# TEST CHECK VALID VALUES
# ─────────────────────────────────

def test_check_valid_values_flags_invalid_status(spark):
    """Invalid order status should be flagged."""
    data = [("delivered",), ("cancelled",), ("invalid_status",)]
    df = spark.createDataFrame(data, ["order_status"])
    result = check_valid_values(df, {
        "order_status": ["delivered", "pending", "cancelled", "returned"]
    })
    bad = result.filter(col("rejection_reason").isNotNull())
    assert bad.count() == 1
    assert bad.first()["rejection_reason"] == "invalid_order_status"


def test_check_valid_values_passes_valid_records(spark):
    """Valid order statuses should not be flagged."""
    data = [("delivered",), ("pending",), ("cancelled",)]
    df = spark.createDataFrame(data, ["order_status"])
    result = check_valid_values(df, {
        "order_status": ["delivered", "pending", "cancelled", "returned"]
    })
    bad = result.filter(col("rejection_reason").isNotNull())
    assert bad.count() == 0

# ─────────────────────────────────
# TEST QUARANTINE RECORDS
# ─────────────────────────────────

def test_quarantine_records_splits_correctly(spark):
    """Good and bad records should be split correctly."""
    data = [("order1", "cust1"), ("order2", None)]
    df = spark.createDataFrame(data, ["order_id", "customer_id"])
    df = check_nulls(df, ["customer_id"])
    good_df, bad_df = quarantine_records(df)
    assert good_df.count() == 1
    assert bad_df.count() == 1
    assert "rejection_reason" not in good_df.columns


def test_quarantine_records_all_good(spark):
    """All good records should go to good_df."""
    data = [("order1", "cust1"), ("order2", "cust2")]
    df = spark.createDataFrame(data, ["order_id", "customer_id"])
    df = check_nulls(df, ["customer_id"])
    good_df, bad_df = quarantine_records(df)
    assert good_df.count() == 2
    assert bad_df.count() == 0
