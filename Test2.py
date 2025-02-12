
F.when(F.col("some_column").isNull(), F.lit(0)).otherwise(F.col("some_column"))



It depends on how you want to treat NULL values in your conditional logic.

If you are okay with “missing” (NULL) columns being interpreted as False, then COALESCE is the simplest, most explicit way to do it.

If you do not use COALESCE, then any row missing those columns will wind up with NULL. This can cause your conditions in Spark SQL to evaluate to NULL rather than False, which might yield unexpected results.

def test_mismatched_ids(spark):
    """
    Ensures that if 'recoveries_df' or 'customer_df' does NOT have a matching record,
    we still handle NULL columns gracefully.
    """
    # Account has "A100"
    account_data = [
        Row(account_id="A100", 
            is_account_paid_in_full=0,
            post_charge_off_account_settled_in_full_notification=0,
            pre_charge_off_account_settled_in_full_notification=0,
            posted_balance=999,
            last_reported_1099_amount=123
        ),
    ]

    # recoveries_df has NO matching row for "A100" -> those columns will be NULL
    rec_data = [
        Row(account_id="B200", asset_sales_notification=1)  # Some unrelated ID
    ]

    # customer_df also doesn't match "A100"
    cust_data = [
        Row(account_id="C300", bankruptcy_status="Open", bankruptcy_chapter="13")
    ]

    account_df = spark.createDataFrame(account_data)
    rec_df = spark.createDataFrame(rec_data)
    cust_df = spark.createDataFrame(cust_data)

    # Now call your function
    result_df = calculate_current_balance(account_df, rec_df, cust_df)
    result = result_df.collect()[0]
    # Confirm we didn't crash and see what the "current_balance_amount" is.
    
    # For example, if the code treats missing data as booleans = False,
    # none of the "is_account_paid_in_full" or "asset_sales_notification" 
    # conditions would be triggered. posted_balance=999 means it's > 0,
    # so if there's no SIF/PIF, no bankruptcy, we might end up in 
    # .otherwise(F.col(CURRENT_BALANCE) - F.col(LAST_1099_AMOUNT))  or
    # .otherwise(F.col(LAST_1099_AMOUNT)) 
    # depending on your code. Let's say you expect 123:
    assert result["current_balance_amount"] == 123

________

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
    Joins account_df, recoveries_df, customer_df on account_id (left joins),
    then applies the business logic for current_balance_amount.
    In Option A, we coalesce each potentially-null column to 0 before
    casting to boolean, so that missing records become False.
    """

    try:
        # 1) Join the three DFs on "account_id" using left joins
        joined_df = (
            account_df.alias("acc")
            .join(recoveries_df.alias("rec"), on="account_id", how="left")
            .join(customer_df.alias("cust"), on="account_id", how="left")
        )

        # 2) Cast integer columns to booleans, but coalesce to 0 first
        calculated_df = (
            joined_df
            # Coalesce to 0 for missing or null values
            .withColumn(
                PIF_NOTIFICATION,
                F.coalesce(F.col("is_account_paid_in_full"), F.lit(0)).cast("boolean")
            )
            .withColumn(
                SIF_NOTIFICATION,
                F.coalesce(
                    F.col("post_charge_off_account_settled_in_full_notification"),
                    F.lit(0)
                ).cast("boolean")
            )
            .withColumn(
                PRE_CO_SIF_NOTIFICATION,
                F.coalesce(
                    F.col("pre_charge_off_account_settled_in_full_notification"),
                    F.lit(0)
                ).cast("boolean")
            )
            .withColumn(
                ASSET_SALES_NOTIFICATION,
                F.coalesce(F.col("asset_sales_notification"), F.lit(0)).cast("boolean")
            )
        )

        # 3) Define the logic for calculating the current balance.
        #    Adjust the logic as needed; shown here as an example.
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
            # Example of otherwise subtracting last_1099 from posted_balance;
            # If your real logic is just .otherwise(F.col(LAST_1099_AMOUNT)), adjust accordingly.
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
