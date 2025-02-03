
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, trim

def calculate_account_type(input_df: DataFrame) -> DataFrame:
    # No extra column—just trim inline:
    account_type_col = (
        when(trim(col("credit_limit")) == "", None)
        .when(
            (col("product_type") == "private_label_partnership") & 
            (trim(col("credit_limit")) == "NPSL"),
            None
        )
        .when(trim(col("credit_limit")) == "NPSL", "0G")
        .when(col("product_type") == "small_business", "8A")
        .when(col("product_type") == "private_label_partnership", "07")
        .otherwise("18")
    )

    # Keep only the columns you need
    return input_df.select(
        "account_id",
        account_type_col.alias("account_type")
    )


-------




import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
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


def _check_results(data, spark, schema=None):
    """
    Utility method that:
      1) Creates a DataFrame (with an optional explicit schema)
      2) Calls calculate_account_type
      3) Collects results
      4) Asserts correctness of each row's account_type

    Expects 'data' to be a list of tuples:
      (credit_limit, product_type, account_id, expected_account_type)
    """
    # If we have a schema, create the DataFrame that way:
    if schema is not None:
        input_df = spark.createDataFrame(data, schema=schema)
    else:
        # Otherwise let Spark infer from the data
        columns = ["credit_limit", "product_type", "account_id", "expected_account_type"]
        input_df = spark.createDataFrame(data, columns)

    # Apply your logic
    result_df = calculate_account_type(input_df)

    # Collect to Python
    rows = result_df.orderBy("account_id").collect()
    actual_map = {r["account_id"]: r["account_type"] for r in rows}

    # Check each row
    for row in data:
        credit_limit, product_type, acc_id, expected = row
        actual = actual_map[acc_id]
        assert actual == expected, (
            f"For account_id={acc_id}, credit_limit='{credit_limit}', product_type='{product_type}', "
            f"expected={expected}, got={actual}"
        )


def test_blank_credit_limit(spark):
    """
    If assigned credit limit is an empty or whitespace string, 
    we expect the output to be None (NULL).
    """
    data = [
        ("",    "small_business", 50, None),
        ("  ",  "random_type",    51, None),
    ]

    # Explicit schema so Spark doesn't throw "CANNOT_DETERMINE_TYPE"
    schema = StructType([
        StructField("credit_limit",            StringType(),  True),
        StructField("product_type",            StringType(),  True),
        StructField("account_id",              IntegerType(), True),
        StructField("expected_account_type",   StringType(),  True),
    ])

    # Use _check_results with the schema
    _check_results(data, spark, schema=schema)
