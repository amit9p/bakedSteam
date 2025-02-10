
SELECT COUNT(DISTINCT column_name) AS distinct_count
FROM table_name;


SELECT t2.*
FROM tbl2 t2
LEFT JOIN tbl1 t1 
ON t1.id = t2.id  -- Replace 'id' with your primary key or unique column
WHERE t1.id IS NULL;



import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from ecbr_calculations.utils.current_balance import calculate_current_balance

# Define constants
PIF_NOTIFICATION = "is_account_paid_in_full"
SIF_NOTIFICATION = "post_charge_off_account_settled_in_full_notification"
PRE_CO_SIF_NOTIFICATION = "pre_charge_off_account_settled_in_full_notification"
ASSET_SALES_NOTIFICATION = "asset_sales_notification"
CURRENT_BALANCE = "posted_balance"
BANKRUPTCY_STATUS = "bankruptcy_status"
BANKRUPTCY_CHAPTER = "bankruptcy_chapter"
LAST_1099_AMOUNT = "last_reported_1099_amount"

# Pytest Spark Session Fixture
@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.appName("PySpark Unit Testing").getOrCreate()
    yield spark_session
    spark_session.stop()

# Test all branches
def test_all_branches(spark):
    schema = StructType([
        StructField("account_id", IntegerType(), nullable=True),
        StructField(PIF_NOTIFICATION, IntegerType(), nullable=True),
        StructField(SIF_NOTIFICATION, IntegerType(), nullable=True),
        StructField(PRE_CO_SIF_NOTIFICATION, IntegerType(), nullable=True),
        StructField(ASSET_SALES_NOTIFICATION, IntegerType(), nullable=True),
        StructField(CURRENT_BALANCE, IntegerType(), nullable=True),
        StructField(BANKRUPTCY_STATUS, StringType(), nullable=True),
        StructField(BANKRUPTCY_CHAPTER, StringType(), nullable=True),
        StructField(LAST_1099_AMOUNT, IntegerType(), nullable=True),
    ])

    test_data = [
        (1, 1, 0, 0, 0, 1000, "None", "None", 0),  # PIF Notification
        (2, 0, 1, 0, 0, 1000, "None", "None", 0),  # SIF Notification
        (3, 0, 0, 1, 0, 1000, "None", "None", 0),  # Pre-CO SIF Notification
        (4, 0, 0, 0, 1, 1000, "None", "None", 0),  # Asset Sales Notification
        (5, 0, 0, 0, 0, 0, "None", "None", 0),  # Posted Balance <= 0
        (6, 0, 0, 0, 0, 1000, "Open", "13", 0),  # Bankruptcy Open & Chapter 13
        (7, 0, 0, 0, 0, 1000, "Discharged", "None", 0),  # Bankruptcy Discharged
        (8, 0, 0, 0, 0, 800, "Dismissed", "None", 100),  # Bankruptcy Dismissed
        (9, 0, 0, 0, 0, 800, "Closed", "None", 100),  # Bankruptcy Closed
        (10, 0, 0, 0, 0, 800, None, None, 100),  # Null Bankruptcy Status
        (11, 0, 0, 0, 0, 800, "Open", "07", 100),  # Bankruptcy Open & Chapter 07
        (12, 0, 0, 0, 0, 800, "Open", "11", 100),  # Bankruptcy Open & Chapter 11
        (13, 0, 0, 0, 0, 800, "Open", "None", 100),  # Bankruptcy Open with No Chapter
    ]

    input_df = spark.createDataFrame(test_data, schema)
    output_df = calculate_current_balance(input_df).orderBy("account_id")

    results = [(row["account_id"], row["current_balance_amount"]) for row in output_df.collect()]

    expected = [
        (1, 0), (2, 0), (3, 0), (4, 0), (5, 0),
        (6, 0), (7, 0), (8, 700), (9, 700), (10, 700),
        (11, 700), (12, 700), (13, 700),
    ]

    assert results == expected, f"Unexpected results: {results}"
    print("Test all_branches passed!")
