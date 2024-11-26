
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

def test_happy_path():
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    
    # Define the schema explicitly
    schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("credit_utilized", IntegerType(), True),
        StructField("is_converted", BooleanType(), True),
        StructField("conversion_balance", IntegerType(), True),
        StructField("is_charged_off", BooleanType(), True)
    ])
    
    # Create DataFrame using the defined schema
    happy_test_data = [
        (1, 500, False, None, False),
        (1, 700, False, None, False),
        (2, 800, True, 800, False)
    ]
    df = spark.createDataFrame(happy_test_data, schema=schema)
    result_df = calculate_highest_credit_per_account(df)
    expected_data = [
        (1, 700),
        (2, 800)
    ]
    
    # Define the expected schema
    expected_schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("highest_credit", IntegerType(), True)
    ])
    
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert result_df.collect() == expected_df.collect(), "Happy path test failed: The DataFrame does not match the expected output."

# Run the test
test_happy_path()




from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

def test_happy_path():
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    
    # Define the schema explicitly
    schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("credit_utilized", IntegerType(), True),
        StructField("is_converted", BooleanType(), True),
        StructField("conversion_balance", IntegerType(), True),
        StructField("is_charged_off", BooleanType(), True)
    ])
    
    # Create DataFrame using the defined schema
    happy_test_data = [
        (1, 500, False, None, False),
        (1, 700, False, None, False),
        (2, 800, True, 800, False)
    ]
    df = spark.createDataFrame(happy_test_data, schema=schema)
    result_df = calculate_highest_credit_per_account(df)
    expected_data = [
        (1, 700),
        (2, 800)
    ]
    
    # Define the expected schema
    expected_schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("highest_credit", IntegerType(), True)
    ])
    
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    
    assert result_df.collect() == expected_df.collect(), "Happy path test failed: The DataFrame does not match the expected output."

# Run the test
test_happy_path()
