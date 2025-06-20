
result_df = df.withColumn(
    BaseSegment.date_closed.str,
    when(
        check_if_any_are_null(
            col(CCAccount.account_close_date.str),
            col(CCAccount.charge_off_date.str)
        ),
        lit(DEFAULT_ERROR_DATE)
    ).when(
        col(CCAccount.account_close_date.str).isNull() |
        (trim(col(CCAccount.account_close_date.str)) == ""),
        col(CCAccount.charge_off_date.str)
    ).otherwise(
        col(CCAccount.account_close_date.str)
    )
)




from datetime import datetime
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from typedspark import create_partially_filled_dataset

from ecbr_card_self_service.ecbr_calculations.fields.base.date_closed import date_closed
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.ecbr_calculations.constants import DEFAULT_ERROR_DATE


def test_date_closed(spark: SparkSession):
    # Input test data
    data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {
                CCAccount.account_id: "1",
                CCAccount.account_close_date: datetime(2024, 12, 9).date(),
                CCAccount.charge_off_date: datetime(2024, 12, 10).date(),
            },
            {
                CCAccount.account_id: "2",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: datetime(2024, 12, 8).date(),
            },
            {
                CCAccount.account_id: "3",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: None,
            },
            {
                CCAccount.account_id: "4",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: None,
            },
        ]
    )

    result_df = date_closed(data)

    # Expected output
    expected_data = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {
                BaseSegment.account_id: "1",
                BaseSegment.date_closed: datetime(2024, 12, 9).date(),
            },
            {
                BaseSegment.account_id: "2",
                BaseSegment.date_closed: datetime(2024, 12, 8).date(),
            },
            {
                BaseSegment.account_id: "3",
                BaseSegment.date_closed: DEFAULT_ERROR_DATE,
            },
            {
                BaseSegment.account_id: "4",
                BaseSegment.date_closed: DEFAULT_ERROR_DATE,
            },
        ]
    )

    assert_df_equality(result_df, expected_data, ignore_row_order=True)
