
# Positive Test Case
def test_positive_case():
    test_data = [
        [1, 1, 0, 0, "STL", 100, "Open", "BANKRUPTCY_CHAPTER_7"],
        [2, 0, 1, 0, "BD", -200, "Open", "BANKRUPTCY_CHAPTER_11"]
    ]
    input_df = spark.createDataFrame(test_data, SCHEMA)
    result = calculate_current_balance(input_df)
    assert result.collect() == [(1, 0), (2, 0)]
    print("Positive test case passed!")

# Negative Test Case
def test_negative_case():
    test_data = [
        [3, 0, 0, 0, "BD", 300, "Closed", "BANKRUPTCY_CHAPTER_7"],
        [4, 0, 0, 0, "BD", 400, "Open", "BANKRUPTCY_CHAPTER_11"]
    ]
    input_df = spark.createDataFrame(test_data, SCHEMA)
    result = calculate_current_balance(input_df)
    assert result.collect() == [(3, 300), (4, 400)]
    print("Negative test case passed!")

# Edge Test Case
def test_edge_case():
    test_data = [
        [5, 0, 1, 0, "STL", 0, "Open", "BANKRUPTCY_CHAPTER_13"]
    ]
    input_df = spark.createDataFrame(test_data, SCHEMA)
    result = calculate_current_balance(input_df)
    assert result.collect() == [(5, 0)]
    print("Edge test case passed!")



########

from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col
from pyspark.sql.utils import AnalysisException

def calculate_current_balance(input_df: DataFrame) -> DataFrame:
    try:
        # Adding the logic for calculating the current balance
        calculated_df = input_df.withColumn(
            "Calculated Current Balance",
            when(
                col("PIF Notification") |
                col("SIF Notification") |
                col("Asset Sales Notification") |
                (col("Charge Off Reason Code") == "STL") |
                (col("Current Balance of the Account") < 0),
                0
            )
            .when(
                (col("Bankruptcy Status") == "Open") &
                (col("Bankruptcy Chapter") == "BANKRUPTCY_CHAPTER_13"), 0
            )
            .when(col("Bankruptcy Status") == "Discharged", 0)
            .otherwise(col("Current Balance of the Account"))
        )

        return calculated_df.select("account_id", "Calculated Current Balance")

    except AnalysisException as e:
        print(f"AnalysisException: {e}")
        raise

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise
