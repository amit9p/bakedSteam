

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    when, col, trim, regexp_extract, lit, length
)

def calculate_account_type(input_df: DataFrame) -> DataFrame:
    """
    Given a DataFrame with columns:
      - assigned_credit_limit (string or int)
      - product_type (string)
      - account_id (some unique identifier)

    Output: A DataFrame with exactly two columns:
      1) account_id
      2) account_type
    
    Business Logic:
      1) If product_type == "private_label_partnership" AND assigned_credit_limit == "NPSL":
         => None  (NULL)
      2) Else if assigned_credit_limit == "NPSL":
         => '0G'
      3) Else if product_type == "small_business" AND assigned_credit_limit is numeric:
         => '8A'
      4) Else if product_type == "private_label_partnership" AND assigned_credit_limit is numeric:
         => '07'
      5) Else
         => '18'
    """

    # Convert assigned_credit_limit to string so we can check if it's "NPSL" or numeric
    df = input_df.withColumn(
        "assigned_credit_limit_str",
        trim(col("assigned_credit_limit").cast("string"))
    )

    # Expression to check if the assigned_credit_limit is purely numeric
    numeric_pattern = r'^\d+$'  # only digits
    is_numeric_expr = (
        length(regexp_extract(col("assigned_credit_limit_str"), numeric_pattern, 0)) > 0
    )

    account_type_col = (
        when(
            (col("product_type") == "private_label_partnership") &
            (col("assigned_credit_limit_str") == "NPSL"),
            None  # If NPSL + private_label_partnership => NULL
        )
        .when(
            col("assigned_credit_limit_str") == "NPSL",
            "0G"  # If NPSL (and not private_label_partnership) => 0G
        )
        .when(
            (col("product_type") == "small_business") & is_numeric_expr,
            "8A"
        )
        .when(
            (col("product_type") == "private_label_partnership") & is_numeric_expr,
            "07"
        )
        .otherwise("18")
    )

    # Create final DF with only account_id and account_type
    final_df = df.withColumn("account_type", account_type_col).select(
        "account_id",
        "account_type"
    )

    return final_df


#######

import pytest
from pyspark.sql import SparkSession
from account_type_calculator import calculate_account_type

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
    # Prepare sample data covering all conditions
    data = [
        # assigned_credit_limit, product_type, account_id, expected_account_type
        ("NPSL", "small_business",            1, "0G"),   # NPSL => 0G (not private_label_partnership)
        ("1000", "small_business",            2, "8A"),   # numeric + small_business => 8A
        ("5000", "private_label_partnership", 3, "07"),   # numeric + private_label_partnership => 07
        ("NPSL", "private_label_partnership", 4, None),   # NPSL + private_label_partnership => None
        ("ABCDE", "small_business",           5, "18"),   # non-numeric => fallback => 18
        ("1500", "something_else",            6, "18"),   # not in any conditions => 18
    ]

    columns = ["assigned_credit_limit", "product_type", "account_id", "expected_account_type"]
    df = spark.createDataFrame(data, columns)

    # Apply our function
    result_df = calculate_account_type(df)

    # Make sure we only have the two columns we want
    assert set(result_df.columns) == {"account_id", "account_type"}, \
        f"Expected only 'account_id' and 'account_type' but got {result_df.columns}"

    # Collect to Python
    results = result_df.orderBy("account_id").collect()
    
    # Build a dict of account_id => account_type
    actual_map = {row["account_id"]: row["account_type"] for row in results}

    # Verify each row
    for row in data:
        (assigned_credit_limit, product_type, acc_id, expected_acct_type) = row
        actual = actual_map[acc_id]
        assert actual == expected_acct_type, (
            f"For account_id={acc_id}, assigned_credit_limit={assigned_credit_limit}, "
            f"product_type={product_type}: expected {expected_acct_type}, got {actual}"
        )
