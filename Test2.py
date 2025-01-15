
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col

def calculate_current_balance(input_df: DataFrame) -> DataFrame:
    # Adding the logic for calculating the current balance
    calculated_df = input_df.withColumn(
        "Calculated Current Balance",
        when(
            (col("PIF Notification") | col("SIF Notification") | col("Asset Sales Notification"))
            | (col("Charge Off Reason Code") == "STL")
            | (col("Current Balance of the Account") < 0)
            | (
                (col("Bankruptcy Status") == "Open")
                & (col("Bankruptcy Chapter") == "BANKRUPTCY_CHAPTER_13")
            )
            | (col("Bankruptcy Status") == "Discharged"),
            0,
        ).otherwise(col("Current Balance of the Account"))
    )
    return calculated_df.select("account_id", "Calculated Current Balance")


#####
from pyspark.sql import SparkSession

def test_calculate_current_balance():
    spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

    # Positive test case
    test_data_positive = [
        (1, True, False, False, "STL", 100, "Open", "BANKRUPTCY_CHAPTER_7"),
        (2, False, True, False, "BD", -200, "Open", "BANKRUPTCY_CHAPTER_11"),
    ]
    schema = [
        "account_id",
        "PIF Notification",
        "SIF Notification",
        "Asset Sales Notification",
        "Charge Off Reason Code",
        "Current Balance of the Account",
        "Bankruptcy Status",
        "Bankruptcy Chapter",
    ]
    input_df_positive = spark.createDataFrame(test_data_positive, schema)
    result_positive = calculate_current_balance(input_df_positive)
    assert result_positive.collect() == [(1, 0), (2, 0)]

    # Negative test case
    test_data_negative = [
        (3, False, False, False, "BD", 300, "Closed", "BANKRUPTCY_CHAPTER_7"),
        (4, False, False, False, "BD", 400, "Open", "BANKRUPTCY_CHAPTER_11"),
    ]
    input_df_negative = spark.createDataFrame(test_data_negative, schema)
    result_negative = calculate_current_balance(input_df_negative)
    assert result_negative.collect() == [(3, 300), (4, 400)]

    # Edge case
    test_data_edge = [
        (5, False, True, False, "STL", 0, "Open", "BANKRUPTCY_CHAPTER_13")
    ]
    input_df_edge = spark.createDataFrame(test_data_edge, schema)
    result_edge = calculate_current_balance(input_df_edge)
    assert result_edge.collect() == [(5, 0)]

# Run the test
test_calculate_current_balance()

"PySpark implementation and tests completed successfully!"
