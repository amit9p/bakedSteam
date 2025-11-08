
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType
from datetime import datetime
from chispa.dataframe_comparer import assert_df_equality

from ecbr_tenant_card_dfs_ll.ecbr_calculations.fields.date_of_account_information import date_of_account_information
from ecbr_tenant_card_dfs_ll.schemas.ecbr_dfs_account import ECBrCalculatorOutput
from ecbr_tenant_card_dfs_ll.helper_functions import create_partially_filled_dataset


def test_date_of_account_information_status_13_or_64(spark):
    # ---------- Prepare input ----------
    rows = [
        Row(
            account_id=1,
            customer_id=100,
            credit_bureau_account_status="13",
            transaction_date=datetime(2024, 6, 1, 0, 0, 0),
        ),
        Row(
            account_id=2,
            customer_id=200,
            credit_bureau_account_status="64",
            transaction_date=datetime(2024, 6, 2, 0, 0, 0),
        ),
    ]

    df = spark.createDataFrame(rows)

    # ---------- Run function ----------
    result_df = date_of_account_information(df)

    # ---------- Expected output ----------
    expected_df = create_partially_filled_dataset(
        spark,
        ECBrCalculatorOutput,
        data=[
            {
                ECBrCalculatorOutput.account_id: 1,
                ECBrCalculatorOutput.customer_id: 100,
                ECBrCalculatorOutput.date_of_account_information: datetime(2024, 6, 1, 0, 0, 0),
            },
            {
                ECBrCalculatorOutput.account_id: 2,
                ECBrCalculatorOutput.customer_id: 200,
                ECBrCalculatorOutput.date_of_account_information: datetime(2024, 6, 2, 0, 0, 0),
            },
        ],
    ).select(
        ECBrCalculatorOutput.account_id,
        ECBrCalculatorOutput.customer_id,
        ECBrCalculatorOutput.date_of_account_information,
    )

    # ✅ Force expected column to TimestampType
    expected_df = expected_df.withColumn(
        ECBrCalculatorOutput.date_of_account_information.name,
        col(ECBrCalculatorOutput.date_of_account_information.name).cast(TimestampType())
    )

    # ---------- Compare ----------
    assert_df_equality(result_df, expected_df, ignore_row_order=True)
