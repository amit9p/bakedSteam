
schema = StructType([
    StructField("account_id", StringType()),
    StructField("customer_address_postal_code", StringType()),  # matches function
])

data = [
    ("123", "11111"),
    ("456", "22222"),
    # ...
]




git push --force-with-lease origin CT4019T-220-group3-testing



test_data = [
    # account_id, pif, sif, pre_co_sif, asset_sales, posted_bal, bank_status,  chapter, last_1099
    (1, 1, 0, 0, 0, 100, "None",       "None", 0),  # PIF => 0
    (2, 0, 1, 0, 0, 200, "None",       "None", 0),  # SIF => 0
    (3, 0, 0, 1, 0, 300, "None",       "None", 0),  # Pre-CO SIF => 0
    (4, 0, 0, 0, 1, 400, "None",       "None", 0),  # Asset => 0
    (5, 0, 0, 0, 0, -10, "None",       "None", 0),  # posted_balance <= 0 => 0
    (6, 0, 0, 0, 0, 600, "Open",       "13",   0),  # Bankruptcy open & 13 => 0
    (7, 0, 0, 0, 0, 700, "Discharged", "7",    0),  # Discharged => 0
    (8, 0, 0, 0, 0, 800, "None",       "None", 100) # Else => 800 - 100 = 700
]


expected = [
    (1, 0),
    (2, 0),
    (3, 0),
    (4, 0),
    (5, 0),
    (6, 0),
    (7, 0),
    (8, 700),
]




from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col
from pyspark.sql.utils import AnalysisException

# Field/column name constants from your screenshot
PIF_NOTIFICATION           = "is_account_paid_in_full"
SIF_NOTIFICATION           = "account_settled_in_full_notification"
PRE_CO_SIF_NOTIFICATION    = "pre_charge_off_account_settled_in_full_notification"
ASSET_SALES_NOTIFICATION   = "asset_sales_notification"
CHARGE_OFF_REASON_CODE     = "collections_treatment_segment_code"
CURRENT_BALANCE            = "posted_balance"
BANKRUPTCY_STATUS          = "bankruptcy_status"
BANKRUPTCY_CHAPTER         = "bankruptcy_chapter"
LAST_1099_AMOUNT           = "last_reported_1099_amount"  # You may need to add this field in your data

def calculate_current_balance(input_df: DataFrame) -> DataFrame:
    try:
        # Convert relevant columns to Boolean if they are stored as 0/1 integers.
        calculated_df = (
            input_df
            .withColumn(PIF_NOTIFICATION, col(PIF_NOTIFICATION).cast("boolean"))
            .withColumn(SIF_NOTIFICATION, col(SIF_NOTIFICATION).cast("boolean"))
            .withColumn(PRE_CO_SIF_NOTIFICATION, col(PRE_CO_SIF_NOTIFICATION).cast("boolean"))
            .withColumn(ASSET_SALES_NOTIFICATION, col(ASSET_SALES_NOTIFICATION).cast("boolean"))
        )

        # Define the logic for calculating the current balance.
        calculated_df = calculated_df.withColumn(
            "Calculated Current Balance",
            when(
                # 1) PIF, SIF, Pre‐CO SIF, Asset Sales, reason code=STL, or balance <= 0 => 0
                (col(PIF_NOTIFICATION)) |
                (col(SIF_NOTIFICATION)) |
                (col(PRE_CO_SIF_NOTIFICATION)) |
                (col(ASSET_SALES_NOTIFICATION)) |
                (col(CHARGE_OFF_REASON_CODE) == "STL") |
                (col(CURRENT_BALANCE) <= 0),
                0
            )
            .when(
                # 2) Bankruptcy status = Open & chapter=13 => 0
                (col(BANKRUPTCY_STATUS) == "Open") & (col(BANKRUPTCY_CHAPTER) == "13"),
                0
            )
            .when(
                # 3) Bankruptcy status = Discharged => 0
                (col(BANKRUPTCY_STATUS) == "Discharged"),
                0
            )
            .otherwise(
                # 4) Else => posted_balance - last_reported_1099_amount
                col(CURRENT_BALANCE) - col(LAST_1099_AMOUNT)
            )
        )

        # Return just the fields you need; adjust as desired
        return calculated_df.select("account_id", "Calculated Current Balance")

    except AnalysisException as e:
        print(f"AnalysisException: {e}")
        raise

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise


######

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

# Import your function
from ecbr_calculations.utils.current_balance import calculate_current_balance

# Match the constants from calculate_current_balance
PIF_NOTIFICATION           = "is_account_paid_in_full"
SIF_NOTIFICATION           = "account_settled_in_full_notification"
PRE_CO_SIF_NOTIFICATION    = "pre_charge_off_account_settled_in_full_notification"
ASSET_SALES_NOTIFICATION   = "asset_sales_notification"
CHARGE_OFF_REASON_CODE     = "collections_treatment_segment_code"
CURRENT_BALANCE            = "posted_balance"
BANKRUPTCY_STATUS          = "bankruptcy_status"
BANKRUPTCY_CHAPTER         = "bankruptcy_chapter"
LAST_1099_AMOUNT           = "last_reported_1099_amount"

# Provide a Spark session fixture for Pytest
@pytest.fixture(scope="module")
def spark():
    spark_session = (
        SparkSession.builder
        .appName("PySpark Unit Testing")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()

def test_all_branches(spark):
    # Example schema including the new Pre‐CO SIF and last 1099 fields
    schema = StructType([
        StructField("account_id", IntegerType(), nullable=True),
        StructField(PIF_NOTIFICATION, IntegerType(), nullable=True),           # 0/1 -> bool
        StructField(SIF_NOTIFICATION, IntegerType(), nullable=True),           # 0/1 -> bool
        StructField(PRE_CO_SIF_NOTIFICATION, IntegerType(), nullable=True),    # 0/1 -> bool
        StructField(ASSET_SALES_NOTIFICATION, IntegerType(), nullable=True),   # 0/1 -> bool
        StructField(CHARGE_OFF_REASON_CODE, StringType(), nullable=True),
        StructField(CURRENT_BALANCE, IntegerType(), nullable=True),
        StructField(BANKRUPTCY_STATUS, StringType(), nullable=True),
        StructField(BANKRUPTCY_CHAPTER, StringType(), nullable=True),
        StructField(LAST_1099_AMOUNT, IntegerType(), nullable=True),
    ])

    # Each row is designed to test one branch in your logic
    test_data = [
        # account_id, pif, sif, pre_co_sif, asset_sales, reason, posted,  bank_status,   chapter, last_1099
        # 1) pif => 0
        (1, 1, 0, 0, 0, "NONE", 100, "None", "None", 0),
        # 2) sif => 0
        (2, 0, 1, 0, 0, "NONE", 200, "None", "None", 0),
        # 3) pre_co_sif => 0
        (3, 0, 0, 1, 0, "NONE", 300, "None", "None", 0),
        # 4) asset => 0
        (4, 0, 0, 0, 1, "NONE", 400, "None", "None", 0),
        # 5) reason_code=STL => 0
        (5, 0, 0, 0, 0, "STL",  500, "None", "None", 0),
        # 6) posted_balance <= 0 => 0
        (6, 0, 0, 0, 0, "NONE", -10, "None", "None", 0),
        # 7) bankruptcy = Open & 13 => 0
        (7, 0, 0, 0, 0, "NONE", 600, "Open", "13", 0),
        # 8) bankruptcy = Discharged => 0
        (8, 0, 0, 0, 0, "NONE", 700, "Discharged", "7", 0),
        # 9) else => posted_balance - last_1099 => 800 - 100 = 700
        (9, 0, 0, 0, 0, "NONE", 800, "None", "None", 100),
    ]

    input_df = spark.createDataFrame(test_data, schema)
    output_df = calculate_current_balance(input_df).orderBy("account_id")

    # Convert to a list of (account_id, calculated_balance) for easy checking
    results = [
        (row["account_id"], row["Calculated Current Balance"])
        for row in output_df.collect()
    ]

    # We expect the first 8 to be zero, and the last one to be 700
    expected = [
        (1, 0),
        (2, 0),
        (3, 0),
        (4, 0),
        (5, 0),
        (6, 0),
        (7, 0),
        (8, 0),
        (9, 700),
    ]

    assert results == expected, f"Unexpected results: {results}"
    print("Test all_branches passed!")


