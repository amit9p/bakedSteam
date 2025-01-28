

# test_account_type_calculator.py

import pytest
from pyspark.sql import SparkSession
from account_type_calculator import calculate_account_type

@pytest.fixture(scope="session")
def spark():
    # Create a single SparkSession for all tests in this file
    return SparkSession.builder \
        .appName("account_type_test") \
        .master("local[1]") \
        .getOrCreate()


def test_calculate_account_type(spark):
    # Prepare sample data covering all conditions
    data = [
        # assigned_credit_limit, product_type, account_id, expected_account_type
        ("NPSL", "small_business",  1, "0G"),  # If NPSL => 0G (irrespective of product_type)
        ("1000", "small_business",  2, "8A"),  # numeric + small_business => 8A
        ("5000", "private_label_partnership", 3, "07"),  # numeric + private_label_partnership => 07
        ("NPSL", "private_label_partnership", 4, None),  # private_label_partnership + NPSL => None
        ("ABCDE", "small_business", 5, "18"),  # Not numeric => fallback => 18
        ("1500", "something_else", 6, "18"),   # Not in any of the conditions => 18
    ]

    columns = ["assigned_credit_limit", "product_type", "account_id", "expected_account_type"]
    df = spark.createDataFrame(data, columns)

    # Apply our function
    result_df = calculate_account_type(df)

    # Collect back to Python to do assertion
    results = result_df.select("account_id", "account_type", "expected_account_type").orderBy("account_id").collect()

    # Verify each row
    for row in results:
        assert row.account_type == row.expected_account_type, \
            f"For account_id={row.account_id}, expected {row.expected_account_type} but got {row.account_type}"


def test_calculate_account_type_all_numeric(spark):
    # Additional test: all numeric, various product types
    data = [
        ("100", "small_business", 1, "8A"),
        ("200", "private_label_partnership", 2, "07"),
        ("300", "random_type", 3, "18"),
    ]

    columns = ["assigned_credit_limit", "product_type", "account_id", "expected_account_type"]
    df = spark.createDataFrame(data, columns)

    result_df = calculate_account_type(df)
    results = result_df.select("account_id", "account_type", "expected_account_type").orderBy("account_id").collect()

    for row in results:
        assert row.account_type == row.expected_account_type


#####
# account_type_calculator.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    when, col, trim, regexp_extract, lit, length
)


def calculate_account_type(input_df: DataFrame) -> DataFrame:
    """
    Given a DataFrame with columns:
      - assigned_credit_limit (string/int)
      - product_type (string)
      - account_id (some unique identifier)
    Apply the following logic to derive 'account_type':

      1. If assigned_credit_limit == "NPSL" => '0G'
      2. Else if product_type == "small_business" AND assigned_credit_limit is numeric => '8A'
      3. Else if product_type == "private_label_partnership" AND assigned_credit_limit is numeric => '07'
      4. Else if product_type == "private_label_partnership" AND assigned_credit_limit == "NPSL" => None (NULL)
      5. Else => '18'

    Returns the original DataFrame with an additional column 'account_type'.
    """

    # Convert the assigned_credit_limit to string (to handle both numeric and 'NPSL') 
    # Then trim white space just in case.
    df = input_df.withColumn("assigned_credit_limit_str", trim(col("assigned_credit_limit").cast("string")))

    # We can define a small helper expression to check if the assigned_credit_limit is purely numeric.
    # e.g. we can use a regex that extracts digits, then compare length, or use a cast attempt.
    # Here, we use regexp_extract + length to test numeric only:
    numeric_pattern = r'^\d+$'  # Only digits
    is_numeric_expr = (
        length(regexp_extract(col("assigned_credit_limit_str"), numeric_pattern, 0)) > 0
    )

    # Build the account_type column using when/otherwise for your logic
    account_type_col = (
        when(col("assigned_credit_limit_str") == "NPSL", "0G")
        .when(
            (col("product_type") == "small_business") & (is_numeric_expr),
            "8A"
        )
        .when(
            (col("product_type") == "private_label_partnership") & (is_numeric_expr),
            "07"
        )
        .when(
            (col("product_type") == "private_label_partnership") & (col("assigned_credit_limit_str") == "NPSL"),
            None
        )
        .otherwise("18")
    )

    # Attach the new column
    df = df.withColumn("account_type", account_type_col)

    # Drop the helper column if you like, or keep for debugging:
    df = df.drop("assigned_credit_limit_str")

    return df
