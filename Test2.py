

from datetime import datetime, date
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, TimestampType

from ecbr_tenant_card_dfs_ll.ecbr_calculations.fields.date_of_last_payment import date_of_last_payment
from ecbr_tenant_card_dfs_ll.schemas.ecbr_dfs_account import ECBrCalculatorOutput
from ecbr_tenant_card_dfs_ll.schemas.enterprise_credit_bureau_reporting_card_dfs_accounts_primary import ECBrCardDFSAccountsPrimary
from ecbr_tenant_card_dfs_ll.helper_functions import create_partially_filled_dataset


def test_date_of_last_payment(spark):
    # ---------- Input Data ----------
    input_df = create_partially_filled_dataset(
        spark,
        ECBrCardDFSAccountsPrimary,
        data=[
            # Case 1: transaction_date not null (timestamp)
            {
                ECBrCardDFSAccountsPrimary.account_id: 1,
                ECBrCardDFSAccountsPrimary.customer_id: 100,
                ECBrCardDFSAccountsPrimary.transaction_date: datetime(2024, 6, 1, 10, 30, 0),
                ECBrCardDFSAccountsPrimary.pre_chargeoff_last_payment_date: date(2024, 5, 15),
            },
            # Case 2: transaction_date null → use pre_chargeoff_last_payment_date
            {
                ECBrCardDFSAccountsPrimary.account_id: 2,
                ECBrCardDFSAccountsPrimary.customer_id: 200,
                ECBrCardDFSAccountsPrimary.transaction_date: None,
                ECBrCardDFSAccountsPrimary.pre_chargeoff_last_payment_date: date(2024, 5, 20),
            },
            # Case 3: both null
            {
                ECBrCardDFSAccountsPrimary.account_id: 3,
                ECBrCardDFSAccountsPrimary.customer_id: 300,
                ECBrCardDFSAccountsPrimary.transaction_date: None,
                ECBrCardDFSAccountsPrimary.pre_chargeoff_last_payment_date: None,
            },
        ],
    )

    # ---------- Run Function ----------
    result_df = date_of_last_payment(input_df)

    # Extract timestamp dynamically from input_df to match exactly
    tx_time_row = input_df.filter(col("account_id") == 1).select("transaction_date").collect()[0]
    tx_time = tx_time_row["transaction_date"]  # datetime(2024,6,1,10,30,0)

    # ---------- Expected Data ----------
    expected_df = create_partially_filled_dataset(
        spark,
        ECBrCalculatorOutput,
        data=[
            {
                ECBrCalculatorOutput.account_id: 1,
                ECBrCalculatorOutput.customer_id: 100,
                ECBrCalculatorOutput.date_of_last_payment: tx_time,  # dynamic timestamp
            },
            {
                ECBrCalculatorOutput.account_id: 2,
                ECBrCalculatorOutput.customer_id: 200,
                # pre_chargeoff_last_payment_date promoted to timestamp (00:00:00)
                ECBrCalculatorOutput.date_of_last_payment: datetime(2024, 5, 20, 0, 0, 0),
            },
            {
                ECBrCalculatorOutput.account_id: 3,
                ECBrCalculatorOutput.customer_id: 300,
                ECBrCalculatorOutput.date_of_last_payment: None,
            },
        ],
    ).select(
        ECBrCalculatorOutput.account_id,
        ECBrCalculatorOutput.customer_id,
        ECBrCalculatorOutput.date_of_last_payment,
    )

    # ---------- Fix schema mismatch ----------
    expected_df = (
        expected_df
        .withColumn("customer_id", col("customer_id").cast(LongType()))
        .withColumn("date_of_last_payment", col("date_of_last_payment").cast(TimestampType()))
    )

    # ---------- Compare ----------
    assert_df_equality(result_df, expected_df, ignore_row_order=True)
