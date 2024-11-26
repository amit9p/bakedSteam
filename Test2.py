
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

# Setup for Spark Session
spark = SparkSession.builder.master("local[1]").appName("Example").getOrCreate()

# Function to calculate highest credit per account
def calculate_highest_credit_per_account(df):
    # Assume function implementation here
    # Dummy return for illustration
    return df

# Schema definition for input data
schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("credit_utilized", StringType(), True),
    StructField("is_charged_off", BooleanType(), True)
])

# Schema definition for expected data
expected_schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("highest_credit", IntegerType(), True)
])

# Test for unhappy path
def test_unhappy_path():
    unhappy_test_data = [
        (2, "invalid", False),
        (3, None, False),
        (4, "-100", False)
    ]
    df = spark.createDataFrame(unhappy_test_data, schema=schema)
    result_df = calculate_highest_credit_per_account(df)

    expected_data = [
        (2, 0),
        (3, 0),
        (4, 0)
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    assert result_df.collect() == expected_df.collect(), "Unhappy path test failed"

# Test for happy path
def test_happy_path():
    happy_test_data = [
        (1, "500", False),
        (2, "800", False)
    ]
    df = spark.createDataFrame(happy_test_data, schema=schema)
    result_df = calculate_highest_credit_per_account(df)

    expected_data = [
        (1, 500),
        (2, 800)
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    assert result_df.collect() == expected_df.collect(), "Happy path test failed"

# Run the tests
test_happy_path()
test_unhappy_path()
