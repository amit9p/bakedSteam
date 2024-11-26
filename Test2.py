
from pyspark.sql.functions import col, when, max

def calculate_highest_credit_per_account(df):
    # Ensure all edge cases are converted to zero
    df = df.withColumn('credit_utilized', 
                       when(col('credit_utilized').cast("integer").isNull() | 
                            (col('credit_utilized').cast("integer") < 0), 0)
                       .otherwise(col('credit_utilized').cast("integer")))

    # Print intermediate DataFrame for debugging
    df.show()

    df_filtered = df.filter(~col('is_charged_off'))
    df_max_credit = df_filtered.groupBy('account_id').agg(max('credit_utilized').alias('highest_credit'))

    # Print final DataFrame before returning
    df_max_credit.show()
    
    return df_max_credit



def test_unhappy_path():
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()

    schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("credit_utilized", StringType(), True),  # Allowing string to simulate bad inputs
        StructField("is_charged_off", BooleanType(), True)
    ])

    unhappy_test_data = [
        (2, "invalid", False),  # Non-integer should convert to 0
        (3, None, False),       # Null should convert to 0
        (4, "-100", False)      # Negative should convert to 0
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

    print("Actual Results:", result_df.collect())
    print("Expected Results:", expected_df.collect())

    assert result_df.collect() == expected_df.collect(), "Unhappy path test failed"

# Run the test
test_unhappy_path()
