
import pytest
from pyspark.sql import SparkSession
from ecbr_calculations.utils.account_type import calculate_account_type


@pytest.fixture(scope="module")
def spark():
    """Provide a shared SparkSession for all tests in this module."""
    spark_session = SparkSession.builder \
        .appName("PySpark Unit Testing - Account Type") \
        .master("local[1]") \
        .getOrCreate()
    return spark_session


def _check_results(data, spark):
    """
    Utility method to create a DataFrame from 'data', run 'calculate_account_type',
    collect results, and assert correctness of the 'account_type' for each row.

    Expects 'data' to be a list of tuples:
        (credit_limit, product_type, account_id, expected_account_type)
    """
    columns = ["credit_limit", "product_type", "account_id", "expected_account_type"]
    input_df = spark.createDataFrame(data, columns)

    # Apply our function
    result_df = calculate_account_type(input_df)

    # Assert the function returns exactly the two columns: account_id, account_type
    assert set(result_df.columns) == {"account_id", "account_type"}, (
        f"Expected columns {{'account_id','account_type'}}, got {result_df.columns}"
    )

    # Collect to Python and verify row-by-row
    rows = result_df.orderBy("account_id").collect()
    actual_map = {r["account_id"]: r["account_type"] for r in rows}

    for row in data:
        credit_limit, product_type, acc_id, expected = row
        actual = actual_map[acc_id]
        assert actual == expected, (
            f"For account_id={acc_id}, credit_limit={credit_limit}, product_type={product_type}, "
            f"expected {expected} but got {actual}"
        )


def test_npsl_vs_private_label_partnership(spark):
    """
    Verify that for credit_limit='NPSL' AND product_type='private_label_partnership',
    we get account_type=None (NULL), which takes priority over the general NPSL => '0G' rule.
    """
    data = [
        ("NPSL", "private_label_partnership", 1, None),  # private_label_partnership + NPSL => None
        ("NPSL", "small_business",            2, "0G"),  # NPSL (not PLP) => '0G'
    ]
    _check_results(data, spark)


def test_numeric_small_business(spark):
    """Check numeric credit_limit + product_type='small_business' => '8A'."""
    data = [
        ("1000", "small_business", 1, "8A"), 
        ("9999", "small_business", 2, "8A"),
        ("NPSL", "small_business", 3, "0G"), # included for cross-check (still NPSL => '0G')
    ]
    _check_results(data, spark)


def test_numeric_private_label_partnership(spark):
    """Check numeric credit_limit + product_type='private_label_partnership' => '07'."""
    data = [
        ("5000", "private_label_partnership", 1, "07"),
        ("123",  "private_label_partnership", 2, "07"),
    ]
    _check_results(data, spark)


def test_fallback_18(spark):
    """
    If it doesn't match any special conditions, it should be '18'.
    That includes non-numeric values (except NPSL), unknown product_type, etc.
    """
    data = [
        ("ABCDE", "small_business",      1, "18"),  # Non-numeric => fallback
        ("2000",  "unknown_type",        2, "18"),  # product_type not in (small_business, private_label_partnership)
        ("something", "private_label_partnership", 3, "18"), # non-numeric w/ PLP
    ]
    _check_results(data, spark)


def test_edge_cases(spark):
    """
    Additional edge scenarios: empty string, negative number, leading/trailing spaces, zero, etc.
    """
    data = [
        ("",      "small_business",            1, "18"),  # empty => not numeric => fallback
        ("   ",   "private_label_partnership", 2, "18"),  # just spaces => fallback
        ("-123",  "small_business",            3, "18"),  # negative => fails the 'digits only' regex => fallback
        ("0000",  "small_business",            4, "8A"),  # all digits => numeric => '8A'
        ("  NPSL  ", "private_label_partnership", 5, None),# trimmed NPSL => None
        ("0",     "private_label_partnership", 6, "07"),  # numeric => '07'
    ]
    _check_results(data, spark)
