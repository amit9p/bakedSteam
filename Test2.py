from pyspark.sql.types import StructType, StructField, StringType, IntegerType, NullType

def test_invalid_input_case():
    schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("PIF Notification", IntegerType(), True),
        StructField("SIF Notification", IntegerType(), True),
        StructField("Asset Sales Notification", IntegerType(), True),
        StructField("Charge Off Reason Code", StringType(), True),
        StructField("Current Balance of the Account", IntegerType(), True),
        StructField("Bankruptcy Status", StringType(), True),
        StructField("Bankruptcy Chapter", StringType(), True)
    ])
    
    test_data = [
        [6, None, 1, None, "INVALID", 500, None, None]
    ]
    
    input_df = spark.createDataFrame(test_data, schema)
    try:
        result = calculate_current_balance(input_df)
        result.collect()
    except Exception as e:
        print(f"Invalid input test case passed with error: {e}")


#####




from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col
from pyspark.sql.utils import AnalysisException

# Define constants to avoid duplication of literals
PIF_NOTIFICATION = "PIF Notification"
SIF_NOTIFICATION = "SIF Notification"
ASSET_SALES_NOTIFICATION = "Asset Sales Notification"
CHARGE_OFF_REASON_CODE = "Charge Off Reason Code"
CURRENT_BALANCE = "Current Balance of the Account"
BANKRUPTCY_STATUS = "Bankruptcy Status"
BANKRUPTCY_CHAPTER = "Bankruptcy Chapter"

def calculate_current_balance(input_df: DataFrame) -> DataFrame:
    try:
        # Cast integer columns to Boolean where necessary
        calculated_df = input_df.withColumn(PIF_NOTIFICATION, col(PIF_NOTIFICATION).cast("boolean")) \
                                .withColumn(SIF_NOTIFICATION, col(SIF_NOTIFICATION).cast("boolean")) \
                                .withColumn(ASSET_SALES_NOTIFICATION, col(ASSET_SALES_NOTIFICATION).cast("boolean"))

        # Adding the logic for calculating the current balance
        calculated_df = calculated_df.withColumn(
            "Calculated Current Balance",
            when(
                col(PIF_NOTIFICATION) |
                col(SIF_NOTIFICATION) |
                col(ASSET_SALES_NOTIFICATION) |
                (col(CHARGE_OFF_REASON_CODE) == "STL") |
                (col(CURRENT_BALANCE) < 0),
                0
            )
            .when(
                (col(BANKRUPTCY_STATUS) == "Open") &
                (col(BANKRUPTCY_CHAPTER) == "BANKRUPTCY_CHAPTER_13"), 0
            )
            .when(col(BANKRUPTCY_STATUS) == "Discharged", 0)
            .otherwise(col(CURRENT_BALANCE))
        )

        return calculated_df.select("account_id", "Calculated Current Balance")

    except AnalysisException as e:
        print(f"AnalysisException: {e}")
        raise

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise



###
def test_positive_case():
    test_data = [
        [1, 1, 0, 0, "STL", 100, "Open", "BANKRUPTCY_CHAPTER_7"],
        [2, 0, 1, 0, "BD", -200, "Open", "BANKRUPTCY_CHAPTER_11"]
    ]
    input_df = spark.createDataFrame(test_data, SCHEMA)
    result = calculate_current_balance(input_df)
    assert result.collect() == [(1, 0), (2, 0)]
    print("Positive test case passed!")

def test_negative_case():
    test_data = [
        [3, 0, 0, 0, "BD", 300, "Closed", "BANKRUPTCY_CHAPTER_7"],
        [4, 0, 0, 0, "BD", 400, "Open", "BANKRUPTCY_CHAPTER_11"]
    ]
    input_df = spark.createDataFrame(test_data, SCHEMA)
    result = calculate_current_balance(input_df)
    assert result.collect() == [(3, 300), (4, 400)]
    print("Negative test case passed!")

def test_edge_case():
    test_data = [
        [5, 0, 1, 0, "STL", 0, "Open", "BANKRUPTCY_CHAPTER_13"]
    ]
    input_df = spark.createDataFrame(test_data, SCHEMA)
    result = calculate_current_balance(input_df)
    assert result.collect() == [(5, 0)]
    print("Edge test case passed!")

def test_invalid_input_case():
    test_data = [
        [6, None, 1, None, "INVALID", 500, None, None]
    ]
    input_df = spark.createDataFrame(test_data, SCHEMA)
    try:
        result = calculate_current_balance(input_df)
        result.collect()
    except Exception as e:
        print(f"Invalid input test case passed with error: {e}")



