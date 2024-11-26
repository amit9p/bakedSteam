
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
from pyspark.sql.functions import col, max, when

def calculate_highest_credit_per_account(df):
    # Clean the data: Convert invalid inputs to 0, handle nulls and negatives
    df_cleaned = df.withColumn("credit_utilized", 
                               when(col("credit_utilized").cast("integer").isNull() |
                                    col("credit_utilized").cast("integer") < 0, 0)
                               .otherwise(col("credit_utilized").cast("integer")))

    # Assuming 'is_charged_off' determines whether to consider the row
    df_filtered = df_cleaned.filter(col("is_charged_off") == False)
    
    # Calculate the maximum credit utilized for each account
    return df_filtered.groupBy("account_id").agg(max("credit_utilized").alias("highest_credit"))

# Define the schema
schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("credit_utilized", StringType(), True),
    StructField("is_charged_off", BooleanType(), True)
])

# Define the Spark session
spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

def test_unhappy_path():
    # Create test data with scenarios expected to default to 0
    data = [
        (1, "invalid", False),
        (2, None, False),
        (3, "-100", False),
        (4, "300", True)  # This row should be ignored due to being charged off
    ]
    df = spark.createDataFrame(data, schema)
    result_df = calculate_highest_credit_per_account(df)

    # Define expected results, expecting 0 for invalid inputs and ignoring the charged off account
    expected_data = [(1, 0), (2, 0), (3, 0)]
    expected_df = spark.createDataFrame(expected_data, ["account_id", "highest_credit"])

    # Print actual results for debugging
    result_df.show()

    # Assert to check the test passes
    assert result_df.collect() == expected_df.collect(), "Unhappy path test failed"

test_unhappy_path()
