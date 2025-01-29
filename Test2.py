
import pytest
from pyspark.sql import SparkSession
from ecbr_calculations.utils.account_type import calculate_account_type

@pytest.fixture(scope="module")
def spark():
    """
    Provide a SparkSession for all tests in this module.
    """
    spark_session = SparkSession.builder \
        .appName("PySpark Unit Testing") \
        .master("local[1]") \
        .getOrCreate()
    return spark_session


def test_calculate_account_type(spark):
    """
    Test that 'calculate_account_type' correctly derives the 'account_type'
    column from 'credit_limit' and 'product_type'.
    """
    # Prepare sample data covering all conditions
    data = [
        # credit_limit, product_type, account_id, expected_account_type
        ("NPSL", "small_business",            1, "0G"),    # NPSL => 0G (if not private_label_partnership)
        ("1000", "small_business",            2, "8A"),    # numeric + small_business => 8A
        ("5000", "private_label_partnership", 3, "07"),    # numeric + private_label_partnership => 07
        ("NPSL", "private_label_partnership", 4, None),    # NPSL + private_label_partnership => None
        ("ABCDE", "small_business",           5, "18"),    # non-numeric => fallback => 18
        ("1500", "something_else",            6, "18"),    # not any special condition => 18
    ]
    columns = ["credit_limit", "product_type", "account_id", "expected_account_type"]
    df = spark.createDataFrame(data, columns)

    # Apply our function under test
    result_df = calculate_account_type(df)

    # The function returns exactly two columns: account_id, account_type
    assert set(result_df.columns) == {"account_id", "account_type"}, (
        f"Expected columns {{'account_id', 'account_type'}}, got {result_df.columns}"
    )

    # Collect results for assertion
    results = result_df.orderBy("account_id").collect()

    # Convert to a dict: account_id -> actual_account_type
    actual_map = {row["account_id"]: row["account_type"] for row in results}

    # Check each expected value
    for row in data:
        (credit_limit, product_type, acc_id, expected_acct_type) = row
        actual = actual_map[acc_id]
        assert actual == expected_acct_type, (
            f"For account_id={acc_id}, credit_limit={credit_limit}, "
            f"product_type={product_type}, expected {expected_acct_type}, got {actual}."
        )
