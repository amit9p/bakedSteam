
SELECT *
FROM Table2 t2
JOIN Table1 t1
ON t2.LOAN_ACCT_NUM = t1.LOAN_ACCT_NUM
AND t2.SLTN_ENRLMT_ID = t1.SLTN_ENRLMT_ID
AND t2.EVT_TYPE_TXT = t1.EVT_TYPE_TXT;




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
    Demonstrates handling NULL columns (due to left joins) using isNull + WHEN/OTHERWISE 
    to assign default values before applying the final logic.
    """

    try:
        # 1) Join the three DFs on "account_id" using left joins
        joined_df = (
            account_df.alias("acc")
            .join(recoveries_df.alias("rec"), on="account_id", how="left")
            .join(customer_df.alias("cust"), on="account_id", how="left")
        )

        # 2) For each column that might be NULL, use isNull + WHEN/OTHERWISE to set a default.
        #    Example: boolean flags -> 0 -> cast("boolean"), numeric -> 0, string -> "" (adjust as needed).

        # Boolean flags originally 0/1 -> default them to 0 if null
        calculated_df = (
            joined_df
            .withColumn(
                PIF_NOTIFICATION,
                F.when(F.col("is_account_paid_in_full").isNull(), F.lit(0))
                 .otherwise(F.col("is_account_paid_in_full"))
                 .cast("boolean")
            )
            .withColumn(
                SIF_NOTIFICATION,
                F.when(F.col("post_charge_off_account_settled_in_full_notification").isNull(), F.lit(0))
                 .otherwise(F.col("post_charge_off_account_settled_in_full_notification"))
                 .cast("boolean")
            )
            .withColumn(
                PRE_CO_SIF_NOTIFICATION,
                F.when(F.col("pre_charge_off_account_settled_in_full_notification").isNull(), F.lit(0))
                 .otherwise(F.col("pre_charge_off_account_settled_in_full_notification"))
                 .cast("boolean")
            )
            .withColumn(
                ASSET_SALES_NOTIFICATION,
                F.when(F.col("asset_sales_notification").isNull(), F.lit(0))
                 .otherwise(F.col("asset_sales_notification"))
                 .cast("boolean")
            )
        )

        # Numeric columns -> default them to 0 if null (example: posted_balance, last_reported_1099_amount)
        calculated_df = (
            calculated_df
            .withColumn(
                CURRENT_BALANCE,
                F.when(F.col(CURRENT_BALANCE).isNull(), F.lit(0))
                 .otherwise(F.col(CURRENT_BALANCE))
            )
            .withColumn(
                LAST_1099_AMOUNT,
                F.when(F.col(LAST_1099_AMOUNT).isNull(), F.lit(0))
                 .otherwise(F.col(LAST_1099_AMOUNT))
            )
        )

        # String columns -> default them to "" if null (example: bankruptcy_status/chapter)
        calculated_df = (
            calculated_df
            .withColumn(
                BANKRUPTCY_STATUS,
                F.when(F.col(BANKRUPTCY_STATUS).isNull(), F.lit(""))
                 .otherwise(F.col(BANKRUPTCY_STATUS))
            )
            .withColumn(
                BANKRUPTCY_CHAPTER,
                F.when(F.col(BANKRUPTCY_CHAPTER).isNull(), F.lit(""))
                 .otherwise(F.col(BANKRUPTCY_CHAPTER))
            )
        )

        # 3) Apply the logic to compute current_balance_amount
        #    (Adjust as needed for your actual requirements)
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
            # Example "otherwise"—subtract last_1099 from posted_balance. 
            # Or use .otherwise(F.col(LAST_1099_AMOUNT)) if that's your actual logic.
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
