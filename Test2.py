
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
from pyspark.sql import SparkSession

def test_unhappy_path():
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    
    # Define the schema explicitly
    schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("credit_utilized", StringType(), True),  # Allowing string to simulate bad input
        StructField("is_converted", BooleanType(), True),
        StructField("conversion_balance", IntegerType(), True),
        StructField("is_charged_off", BooleanType(), True)
    ])
    
    # Unhappy Test Data: Includes invalid, null, and negative values
    unhappy_test_data = [
        (2, "invalid", False, None, False),  # Non-integer value
        (3, None, False, None, False),       # Missing value
        (4, -100, False, None, False)        # Negative value
    ]
    df = spark.createDataFrame(unhappy_test_data, schema=schema)
    result_df = calculate_highest_credit_per_account(df)
    
    # Define the expected schema for result
    expected_schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("highest_credit", IntegerType(), True)
    ])
    
    # Expected Data: Assuming function sets incorrect or missing values to zero
    expected_data = [
        (2, 0),  # Assumes non-integer handled as zero
        (3, 0),  # Missing value handled as zero
        (4, 0)   # Negative value treated as zero or excluded
    ]
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert result_df.collect() == expected_df.collect(), "Unhappy path test failed: The DataFrame does not match the expected output."

# Run the test
test_unhappy_path()
