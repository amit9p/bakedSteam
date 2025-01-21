from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def test_analysis_exception():
    # Define the schema with incorrect column names to simulate an AnalysisException
    schema = StructType([
        StructField("wrong_account_id", IntegerType(), True),
        StructField("wrong_PIF_Notification", IntegerType(), True),
        StructField("wrong_SIF_Notification", IntegerType(), True),
        StructField("wrong_Asset_Sales_Notification", IntegerType(), True),
        StructField("wrong_Charge_Off_Reason_Code", StringType(), True),
        StructField("wrong_Current_Balance_of_the_Account", IntegerType(), True),
        StructField("wrong_Bankruptcy_Status", StringType(), True),
        StructField("wrong_Bankruptcy_Chapter", StringType(), True)
    ])
    
    # Test data with the same number of columns as the schema
    test_data = [
        [1, 1, 0, 0, "STL", 100, "Open", "BANKRUPTCY_CHAPTER_7"]
    ]
    
    # Create a DataFrame with the incorrect schema
    input_df = spark.createDataFrame(test_data, schema)
    
    try:
        # This should trigger an AnalysisException because of column name mismatch
        calculate_current_balance(input_df)
    except AnalysisException as e:
        print(f"AnalysisException test passed with error: {e}")


*******
def test_analysis_exception():
    test_data = [
        [1, 1, 0, 0, "STL", 100, "Open", "BANKRUPTCY_CHAPTER_7"]
    ]
    
    schema = StructType([
        StructField("invalid_column", IntegerType(), True)
    ])
    
    input_df = spark.createDataFrame(test_data, schema)
    
    try:
        calculate_current_balance(input_df)
    except AnalysisException as e:
        print(f"AnalysisException test passed with error: {e}")


def test_generic_exception():
    test_data = [
        [None, None, None, None, None, None, None, None]
    ]
    
    schema = StructType([
        StructField("account_id", NullType(), True),
        StructField("PIF Notification", NullType(), True),
        StructField("SIF Notification", NullType(), True),
        StructField("Asset Sales Notification", NullType(), True),
        StructField("Charge Off Reason Code", NullType(), True),
        StructField("Current Balance of the Account", NullType(), True),
        StructField("Bankruptcy Status", NullType(), True),
        StructField("Bankruptcy Chapter", NullType(), True)
    ])
    
    input_df = spark.createDataFrame(test_data, schema)
    
    try:
        calculate_current_balance(input_df)
    except Exception as e:
        print(f"Generic exception test passed with error: {e}")


%%%%%







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



