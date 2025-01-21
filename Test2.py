
from pyspark.sql import SparkSession
from ecbr_calculations.utils.current_balance_field_21 import calculate_current_balance

# Define constants to avoid duplicating strings
PIF_NOTIFICATION = "PIF Notification"
SIF_NOTIFICATION = "SIF Notification"
ASSET_SALES_NOTIFICATION = "Asset Sales Notification"
CHARGE_OFF_REASON_CODE = "Charge Off Reason Code"
CURRENT_BALANCE = "Current Balance of the Account"
BANKRUPTCY_STATUS = "Bankruptcy Status"
BANKRUPTCY_CHAPTER = "Bankruptcy Chapter"

# Schema as a constant
SCHEMA = [
    "account_id",
    PIF_NOTIFICATION,
    SIF_NOTIFICATION,
    ASSET_SALES_NOTIFICATION,
    CHARGE_OFF_REASON_CODE,
    CURRENT_BALANCE,
    BANKRUPTCY_STATUS,
    BANKRUPTCY_CHAPTER
]

# Setup Spark session
spark = SparkSession.builder.appName("My App").getOrCreate()

# Positive Test Case
def test_positive_case():
    test_data = [
        [1, True, False, False, "STL", 100, "Open", "BANKRUPTCY_CHAPTER_7"],
        [2, False, True, False, "BD", -200, "Open", "BANKRUPTCY_CHAPTER_11"]
    ]
    input_df = spark.createDataFrame(test_data, SCHEMA)
    result = calculate_current_balance(input_df)
    assert result.collect() == [(1, 0), (2, 0)]
    print("Positive test case passed!")

# Negative Test Case
def test_negative_case():
    test_data = [
        [3, False, False, False, "BD", 300, "Closed", "BANKRUPTCY_CHAPTER_7"],
        [4, False, False, False, "BD", 400, "Open", "BANKRUPTCY_CHAPTER_11"]
    ]
    input_df = spark.createDataFrame(test_data, SCHEMA)
    result = calculate_current_balance(input_df)
    assert result.collect() == [(3, 300), (4, 400)]
    print("Negative test case passed!")

# Edge Test Case
def test_edge_case():
    test_data = [
        [5, False, True, False, "STL", 0, "Open", "BANKRUPTCY_CHAPTER_13"]
    ]
    input_df = spark.createDataFrame(test_data, SCHEMA)
    result = calculate_current_balance(input_df)
    assert result.collect() == [(5, 0)]
    print("Edge test case passed!")
