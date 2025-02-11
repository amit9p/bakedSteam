import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.utils import AnalysisException

# Suppose this is your function
# from my_module import calculate_current_balance

def test_analysis_exception(spark):
    # DataFrames missing required columns
    account_data = [ Row(account_id="X1") ]  # e.g. has no "posted_balance" column
    rec_data = [ Row(account_id="X1") ]
    cust_data = [ Row(account_id="X1") ]

    account_df = spark.createDataFrame(account_data)
    rec_df = spark.createDataFrame(rec_data)
    cust_df = spark.createDataFrame(cust_data)

    # Expect an AnalysisException because "posted_balance" (or some required column)
    # does not exist in account_df
    with pytest.raises(AnalysisException):
        calculate_current_balance(account_df, rec_df, cust_df)

def test_generic_exception(spark):
    # Passing None or a non-DataFrame argument is likely to raise a generic Exception
    # if your code tries to call methods on it.
    with pytest.raises(Exception, match="unexpected error"):
        calculate_current_balance(None, None, None)





from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# Constants
PIF_NOTIFICATION = "pif_notification"
SIF_NOTIFICATION = "sif_notification"
PRE_CO_SIF_NOTIFICATION = "pre_co_sif_notification"
ASSET_SALES_NOTIFICATION = "asset_sales_notification"
CURRENT_BALANCE = "posted_balance"
BANKRUPTCY_STATUS = "bankruptcy_status"
BANKRUPTCY_CHAPTER = "bankruptcy_chapter"
LAST_1099_AMOUNT = "last_reported_1099_amount"


def calculate_current_balance(
    account_df: DataFrame,
    recoveries_df: DataFrame,
    customer_df: DataFrame
) -> DataFrame:
    """
    Joins account_df, recoveries_df, customer_df on account_id (assumed common key),
    then applies the business logic to compute current_balance_amount.
    """

    try:
        # 1) Join the three DFs on "account_id".
        #    Adjust 'how' (e.g. "inner", "left") as needed by your actual use-case.
        joined_df = (
            account_df.alias("acc")
            .join(recoveries_df.alias("rec"), on="account_id", how="left")
            .join(customer_df.alias("cust"), on="account_id", how="left")
        )

        # 2) Cast integer columns to booleans, rename them for clarity
        calculated_df = (
            joined_df
            .withColumn(
                PIF_NOTIFICATION,
                F.col("is_account_paid_in_full").cast("boolean")
            )
            .withColumn(
                SIF_NOTIFICATION,
                F.col("post_charge_off_account_settled_in_full_notification").cast("boolean")
            )
            .withColumn(
                PRE_CO_SIF_NOTIFICATION,
                F.col("pre_charge_off_account_settled_in_full_notification").cast("boolean")
            )
            .withColumn(
                ASSET_SALES_NOTIFICATION,
                F.col("asset_sales_notification").cast("boolean")
            )
        )

        # 3) Define the logic for calculating the current balance.
        calculated_df = calculated_df.withColumn(
            "current_balance_amount",
            F.when(
                (F.col(PIF_NOTIFICATION)) |
                (F.col(SIF_NOTIFICATION)) |
                (F.col(PRE_CO_SIF_NOTIFICATION)) |
                (F.col(ASSET_SALES_NOTIFICATION)) |
                (F.col(CURRENT_BALANCE) <= 0),
                0
            )
            .when(
                (F.col(BANKRUPTCY_STATUS) == "Open") & (F.col(BANKRUPTCY_CHAPTER) == "13"),
                0
            )
            .when(
                F.col(BANKRUPTCY_STATUS) == "Discharged",
                0
            )
            .otherwise(F.col(LAST_1099_AMOUNT))
        )

        # 4) Return the fields you need; adjust as desired
        return calculated_df.select("account_id", "current_balance_amount")

    except AnalysisException as ae:
        print(f"AnalysisException: {ae}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise


######


import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

# Import your function from wherever it lives
# from my_project.my_module import calculate_current_balance


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for all tests."""
    return SparkSession.builder \
        .master("local[*]") \
        .appName("TestCalculateCurrentBalance") \
        .getOrCreate()


def test_all_zero_case(spark):
    """
    Tests the branch where pif / sif / pre_co_sif / asset_sales == True or posted_balance <= 0,
    leading to a result of 0.
    """
    account_data = [
        # posted_balance <= 0 or PIF=1 => expect 0
        Row(account_id="A1", is_account_paid_in_full=1, 
            post_charge_off_account_settled_in_full_notification=0,
            pre_charge_off_account_settled_in_full_notification=0,
            posted_balance=-10,  # negative => should go 0
            last_reported_1099_amount=999),
    ]
    recoveries_data = [
        Row(account_id="A1", asset_sales_notification=0)
    ]
    customer_data = [
        Row(account_id="A1", bankruptcy_status="", bankruptcy_chapter="")
    ]
    account_df = spark.createDataFrame(account_data)
    rec_df = spark.createDataFrame(recoveries_data)
    cust_df = spark.createDataFrame(customer_data)

    result_df = calculate_current_balance(account_df, rec_df, cust_df)
    row = result_df.collect()[0]
    assert row["account_id"] == "A1"
    assert row["current_balance_amount"] == 0


def test_bankruptcy_open_ch13(spark):
    """
    Tests the branch where bankruptcy_status='Open' and bankruptcy_chapter='13',
    leading to current_balance_amount=0.
    """
    account_data = [
        Row(account_id="A2", is_account_paid_in_full=0,
            post_charge_off_account_settled_in_full_notification=0,
            pre_charge_off_account_settled_in_full_notification=0,
            posted_balance=500,
            last_reported_1099_amount=999),
    ]
    recoveries_data = [
        Row(account_id="A2", asset_sales_notification=0)
    ]
    customer_data = [
        Row(account_id="A2", bankruptcy_status="Open", bankruptcy_chapter="13")
    ]
    account_df = spark.createDataFrame(account_data)
    rec_df = spark.createDataFrame(recoveries_data)
    cust_df = spark.createDataFrame(customer_data)

    result_df = calculate_current_balance(account_df, rec_df, cust_df)
    row = result_df.collect()[0]
    assert row["current_balance_amount"] == 0


def test_bankruptcy_discharged(spark):
    """
    Tests the branch where bankruptcy_status='Discharged',
    leading to current_balance_amount=0.
    """
    account_data = [
        Row(account_id="A3", is_account_paid_in_full=0,
            post_charge_off_account_settled_in_full_notification=0,
            pre_charge_off_account_settled_in_full_notification=0,
            posted_balance=1000,
            last_reported_1099_amount=500),
    ]
    rec_data = [
        Row(account_id="A3", asset_sales_notification=0)
    ]
    cust_data = [
        Row(account_id="A3", bankruptcy_status="Discharged", bankruptcy_chapter="")
    ]
    account_df = spark.createDataFrame(account_data)
    rec_df = spark.createDataFrame(rec_data)
    cust_df = spark.createDataFrame(cust_data)

    result_df = calculate_current_balance(account_df, rec_df, cust_df)
    row = result_df.collect()[0]
    assert row["current_balance_amount"] == 0


def test_otherwise_case(spark):
    """
    Tests the 'otherwise' branch (none of the previous conditions met),
    so current_balance_amount = last_reported_1099_amount.
    """
    account_data = [
        Row(account_id="A4", is_account_paid_in_full=0,
            post_charge_off_account_settled_in_full_notification=0,
            pre_charge_off_account_settled_in_full_notification=0,
            posted_balance=999,
            last_reported_1099_amount=123),
    ]
    rec_data = [
        Row(account_id="A4", asset_sales_notification=0)
    ]
    cust_data = [
        Row(account_id="A4", bankruptcy_status="SomethingElse", bankruptcy_chapter="7")
    ]
    account_df = spark.createDataFrame(account_data)
    rec_df = spark.createDataFrame(rec_data)
    cust_df = spark.createDataFrame(cust_data)

    result_df = calculate_current_balance(account_df, rec_df, cust_df)
    row = result_df.collect()[0]
    assert row["current_balance_amount"] == 123

