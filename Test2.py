
from pyspark.sql.functions import col, when, max

def calculate_highest_credit_per_account(df):
    # Ensure all edge cases (null, negative, non-integer) convert to 0
    df = df.withColumn('credit_utilized',
                       when(
                           col('credit_utilized').cast("integer").isNull() |
                           col('credit_utilized').cast("integer") < 0, 
                           0
                       ).otherwise(col('credit_utilized').cast("integer"))
                      )
    
    # Only consider accounts that are not charged off
    df_filtered = df.filter(~col('is_charged_off'))

    # Aggregate to find maximum credit utilized per account
    df_max_credit = df_filtered.groupBy('account_id').agg(max('credit_utilized').alias('highest_credit'))

    return df_max_credit



def test_unhappy_path():
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()

    # Define schema to include type conversion in data setup
    schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("credit_utilized", StringType(), True),
        StructField("is_charged_off", BooleanType(), True)
    ])

    # Test data simulates edge cases: invalid input, null, and negative values
    unhappy_test_data = [
        (2, "invalid", False),
        (3, None, False),
        (4, "-100", False)
    ]
    df = spark.createDataFrame(unhappy_test_data, schema=schema)
    result_df = calculate_highest_credit_per_account(df)

    # Expectations after handling in function
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

    # Debug output before assertion
    print("Actual Results:", result_df.collect())
    print("Expected Results:", expected_df.collect())

    # Assertion checks if the actual results match the expected results
    assert result_df.collect() == expected_df.collect(), "Unhappy path test failed"

# Run the test
test_unhappy_path()
№##########

from pyspark.sql.functions import col, when, max
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

def calculate_highest_credit_per_account(df):
    # Add a column that attempts to cast credit_utilized to integer, defaults to 0 on failure
    df = df.withColumn('credit_utilized_int', 
                       when(col('credit_utilized').cast("integer").isNull(), 0)
                       .otherwise(col('credit_utilized').cast("integer")))

    # Handle negatives and nulls by ensuring all values are non-negative integers
    df = df.withColumn('credit_utilized_clean',
                       when(col('credit_utilized_int') < 0, 0)
                       .otherwise(col('credit_utilized_int')))

    df.show()  # Debug: Show the DataFrame after cleanup

    # Filter out rows where 'is_charged_off' is true
    df_filtered = df.filter(~col('is_charged_off'))

    # Calculate the maximum credit utilized for each account
    df_max_credit = df_filtered.groupBy('account_id').agg(max('credit_utilized_clean').alias('highest_credit'))

    df_max_credit.show()  # Debug: Show the resulting aggregation

    return df_max_credit

# Example usage:
spark = SparkSession.builder.master("local").appName("Example").getOrCreate()
data = [(1, '500', False), (2, 'invalid', False), (3, None, False), (4, '-100', False)]
schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("credit_utilized", StringType(), True),
    StructField("is_charged_off", BooleanType(), True)
])

df = spark.createDataFrame(data, schema=schema)
result_df = calculate_highest_credit_per_account(df)

