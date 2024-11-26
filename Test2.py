def test_unhappy_path():
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    
    schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("credit_utilized", StringType(), True),
        StructField("is_charged_off", BooleanType(), True)
    ])
    
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
    expected_schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("highest_credit", IntegerType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    print("Actual:", result_df.collect())
    print("Expected:", expected_df.collect())
    
    assert result_df.collect() == expected_df.collect(), "Unhappy path test failed"

# Make sure to call this debugging version of the test
test_unhappy_path()


########
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, max
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

def calculate_highest_credit_per_account(df):
    """
    Calculates the highest credit amount utilized per account, handling errors for non-integer, null, or negative values.
    
    :param df: DataFrame containing account history.
    :return: DataFrame with highest credit utilized per account.
    """
    # Clean data: handle nulls, non-integers, and negatives
    df = df.withColumn('credit_utilized', 
                       when(col('credit_utilized').cast("integer").isNull() | (col('credit_utilized').cast("integer") < 0), 0)
                       .otherwise(col('credit_utilized').cast("integer")))

    # Filter out charged-off accounts
    df_filtered = df.filter(~col('is_charged_off'))

    # Compute the maximum credit utilized per account
    df_max_credit = df_filtered.groupBy('account_id').agg(max('credit_utilized').alias('highest_credit'))

    return df_max_credit

# Initialize SparkSession (typically done at the application start-up)
spark = SparkSession.builder.master("local[1]").appName("Example").getOrCreate()



#№###
def test_happy_path():
    # Happy Path: Valid data inputs
    schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("credit_utilized", StringType(), True),  # Using string type to simulate input variation
        StructField("is_charged_off", BooleanType(), True)
    ])
    happy_test_data = [
        (1, "500", False),
        (1, "700", False),
        (2, "800", False)
    ]
    df = spark.createDataFrame(happy_test_data, schema=schema)
    result_df = calculate_highest_credit_per_account(df)
    expected_data = [
        (1, 700),
        (2, 800)
    ]
    expected_schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("highest_credit", IntegerType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert result_df.collect() == expected_df.collect(), "Happy path test failed"

def test_unhappy_path():
    # Unhappy Path: Invalid data inputs
    schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("credit_utilized", StringType(), True),  # Using string type to simulate input variation
        StructField("is_charged_off", BooleanType(), True)
    ])
    unhappy_test_data = [
        (2, "invalid", False),  # Non-integer value
        (3, None, False),       # Null value
        (4, "-100", False)      # Negative value
    ]
    df = spark.createDataFrame(unhappy_test_data, schema=schema)
    result_df = calculate_highest_credit_per_account(df)
    expected_data = [
        (2, 0),  # Assumes non-integer handled as zero
        (3, 0),  # Null value handled as zero
        (4, 0)   # Negative value treated as zero
    ]
    expected_schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("highest_credit", IntegerType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert result_df.collect() == expected_df.collect(), "Unhappy path test failed"

# Running the tests
test_happy_path()
test_unhappy_path()
