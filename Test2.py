


from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pytest
from pyspark.sql import SparkSession
from ecbr_calculations.utils.account_type import calculate_account_type

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder \
        .appName("TestAccountType") \
        .master("local[1]") \
        .getOrCreate()
    return spark_session

def test_plp_npsl(spark):
    data = [
        ("NPSL", "private_label_partnership", 1, None),
        ("NPSL", "private_label_partnership", 2, None),
    ]
    # Define schema so Spark knows expected_account_type is String (can hold null)
    schema = StructType([
        StructField("credit_limit", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("account_id", IntegerType(), True),
        StructField("expected_account_type", StringType(), True),
    ])
    
    df = spark.createDataFrame(data, schema=schema)
    # Apply your function, assert correctness, etc.
    result_df = calculate_account_type(df)
    ...




# test_account_type.py

import pytest
from pyspark.sql import SparkSession
from ecbr_calculations.utils.account_type import calculate_account_type

@pytest.fixture(scope="module")
def spark():
    """
    Provide a shared SparkSession for all tests in this module.
    """
    spark_session = SparkSession.builder \
        .appName("PySpark Unit Testing - Account Type") \
        .master("local[1]") \
        .getOrCreate()
    return spark_session


def _check_results(data, spark):
    """
    Utility that:
      1) Creates a DataFrame from 'data'
      2) Calls calculate_account_type
      3) Asserts correctness of each row's account_type

    'data' should be a list of tuples:
      (credit_limit, product_type, account_id, expected_account_type)
    """
    columns = ["credit_limit", "product_type", "account_id", "expected_account_type"]
    input_df = spark.createDataFrame(data, columns)

    # Run logic
    result_df = calculate_account_type(input_df)

    # Check columns
    assert set(result_df.columns) == {"account_id", "account_type"}, (
        f"Expected columns {{'account_id','account_type'}}, got {result_df.columns}"
    )

    # Collect & map results
    rows = result_df.orderBy("account_id").collect()
    actual_map = {r["account_id"]: r["account_type"] for r in rows}

    # Verify each row
    for row in data:
        (limit, ptype, acc_id, expected) = row
        actual = actual_map[acc_id]
        assert actual == expected, (
            f"For account_id={acc_id}, credit_limit={limit}, product_type={ptype}, "
            f"expected {expected} but got {actual}"
        )


def test_plp_npsl(spark):
    """
    If product_type='private_label_partnership' AND credit_limit='NPSL',
    => account_type=None
    """
    data = [
        ("NPSL", "private_label_partnership", 1, None),
        ("NPSL", "private_label_partnership", 2, None),
    ]
    _check_results(data, spark)


def test_npsl_non_plp(spark):
    """
    If credit_limit='NPSL' but product_type is anything else => '0G'
    """
    data = [
        ("NPSL", "small_business", 10, "0G"),
        ("NPSL", "random_type",   11, "0G"),
        ("NPSL", None,            12, "0G"),  # Even if product_type is null
    ]
    _check_results(data, spark)


def test_small_business(spark):
    """
    If product_type='small_business' (and credit_limit != 'NPSL'),
    => '8A'
    """
    data = [
        ("1000", "small_business", 20, "8A"),
        ("ABCDE", "small_business", 21, "8A"),
        (None, "small_business",   22, "8A"),  # None credit_limit, not 'NPSL', so -> '8A'
    ]
    _check_results(data, spark)


def test_private_label_partnership_non_npsl(spark):
    """
    If product_type='private_label_partnership' (and credit_limit != 'NPSL'),
    => '07'
    """
    data = [
        ("5000", "private_label_partnership", 30, "07"),
        ("ABCDE", "private_label_partnership",31, "07"),
        (None, "private_label_partnership",   32, "07"),
    ]
    _check_results(data, spark)


def test_other_fallback(spark):
    """
    Everything else => '18'
    i.e. product_type not 'small_business' or 'private_label_partnership',
    and credit_limit != 'NPSL'.
    """
    data = [
        ("5000", "unknown_type", 40, "18"),
        ("ABCDE", "another_type",41, "18"),
        (None, None,            42, "18"),  # No product_type, no NPSL => fallback
    ]
    _check_results(data, spark)
