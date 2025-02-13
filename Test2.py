from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# Constants for readability
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
    Joins account_df, recoveries_df, customer_df on 'account_id' (left joins).
    DOES NOT apply any default for NULL columns. 
    If there's no matching row in recoveries_df/customer_df, 
    columns remain NULL, which may lead to a NULL 'current_balance_amount' 
    if none of the conditions are satisfied.
    """
    try:
        # 1) Left join the three DFs on "account_id"
        joined_df = (
            account_df.alias("acc")
            .join(recoveries_df.alias("rec"), on="account_id", how="left")
            .join(customer_df.alias("cust"), on="account_id", how="left")
        )

        # 2) Simply cast columns to boolean, but do NOT replace NULL with 0/False.
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
            # We'll also leave posted_balance, last_reported_1099_amount,
            # bankruptcy_status, etc. as-is (NULL if unmatched).
        )

        # 3) Define the logic for current_balance_amount.
        #    If any condition references a NULL boolean, the expression can become NULL
        #    instead of True/False.  If posted_balance is NULL, arithmetic might yield NULL.
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
            # Example "otherwise" logic. If posted_balance or last_reported_1099_amount
            # is NULL, the result can be NULL.
            .otherwise(F.col(CURRENT_BALANCE) - F.col(LAST_1099_AMOUNT))
        )

        # 4) Return the fields you need
        return calculated_df.select("account_id", "current_balance_amount")

    except AnalysisException as ae:
        print(f"AnalysisException: {ae}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise



import pytest
from pyspark.sql import SparkSession, Row

# from my_module import calculate_current_balance

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("TestCurrentBalanceNoDefaults") \
        .getOrCreate()

def test_mismatched_rows_yield_null_current_balance(spark):
    """
    If an account row does NOT match any row in recoveries_df/customer_df,
    and posted_balance is also NULL, then we expect the final current_balance_amount 
    to be NULL (no default fallback).
    """
    # This row is in account_df only. 'posted_balance' and 'last_reported_1099_amount'
    # are NULL, and we have no fallback => final result is likely NULL.
    account_data = [
        Row(account_id="A5",
            is_account_paid_in_full=None, 
            post_charge_off_account_settled_in_full_notification=None,
            pre_charge_off_account_settled_in_full_notification=None,
            posted_balance=None,
            last_reported_1099_amount=None)
    ]

    # No matching 'A5' => these rows won't match up. 
    rec_data = [
        Row(account_id="X1", asset_sales_notification=1),
    ]
    cust_data = [
        Row(account_id="X1", bankruptcy_status="Open", bankruptcy_chapter="13"),
    ]

    account_df = spark.createDataFrame(account_data)
    rec_df = spark.createDataFrame(rec_data)
    cust_df = spark.createDataFrame(cust_data)

    result_df = calculate_current_balance(account_df, rec_df, cust_df)
    result = result_df.collect()[0]

    assert result["account_id"] == "A5"
    # Because all relevant columns for the 'when(...)' conditions are NULL,
    # and posted_balance is also NULL => we expect the final result to be None.
    assert result["current_balance_amount"] is None



