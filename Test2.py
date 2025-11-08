
import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession

from ecbr_tenant_card_dfs.ll.ecbr_calculations.fields.date_of_last_payment import date_of_last_payment
from ecbr_tenant_card_dfs.ll.schemas.ecbr_dfs_account import ECBrCardDFSAccountsPrimary
from ecbr_tenant_card_dfs.ll.schemas.ecbr_dfs_account import ECBrCalculatorOutput

@pytest.mark.usefixtures("spark")
def test_date_of_last_payment(spark: SparkSession):
    # ---------- Primary input ----------
    primary_df = create_partially_filled_dataset(
        spark,
        ECBrCardDFSAccountsPrimary,
        data=[
            {
                ECBrCardDFSAccountsPrimary.account_id: 1,
                ECBrCardDFSAccountsPrimary.customer_id: 11,
                ECBrCardDFSAccountsPrimary.transaction_date: None,
                ECBrCardDFSAccountsPrimary.pre_chargeoff_last_payment_date: "2023-05-10",
            },
            {
                ECBrCardDFSAccountsPrimary.account_id: 2,
                ECBrCardDFSAccountsPrimary.customer_id: 22,
                ECBrCardDFSAccountsPrimary.transaction_date: "2024-02-01",
                ECBrCardDFSAccountsPrimary.pre_chargeoff_last_payment_date: "2023-09-09",
            },
            {
                ECBrCardDFSAccountsPrimary.account_id: 3,
                ECBrCardDFSAccountsPrimary.customer_id: 33,
                ECBrCardDFSAccountsPrimary.transaction_date: None,
                ECBrCardDFSAccountsPrimary.pre_chargeoff_last_payment_date: None,
            },
        ],
    )

    # ---------- Expected output ----------
    expected_df = create_partially_filled_dataset(
        spark,
        ECBrCalculatorOutput,
        data=[
            {
                ECBrCalculatorOutput.account_id: 1,
                ECBrCalculatorOutput.customer_id: 11,
                ECBrCalculatorOutput.date_of_last_payment_dt: "2023-05-10",
            },
            {
                ECBrCalculatorOutput.account_id: 2,
                ECBrCalculatorOutput.customer_id: 22,
                ECBrCalculatorOutput.date_of_last_payment_dt: "2024-02-01",
            },
            {
                ECBrCalculatorOutput.account_id: 3,
                ECBrCalculatorOutput.customer_id: 33,
                ECBrCalculatorOutput.date_of_last_payment_dt: None,
            },
        ],
    )

    # ---------- Run actual ----------
    result_df = date_of_last_payment(primary_df)

    # ---------- Compare ----------
    assert_df_equality(result_df, expected_df, ignore_row_order=True)
