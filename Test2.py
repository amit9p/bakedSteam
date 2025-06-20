
from datetime import datetime
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from typedspark import create_partially_filled_dataset

from ecbr_card_self_service.ecbr_calculations.fields.base.date_closed import date_closed
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.ecbr_calculations.constants import DEFAULT_ERROR_DATE

def test_date_closed(spark: SparkSession):
    # Input data covering all scenarios
    data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {
                # Valid account_close_date, valid charge_off_date (should use account_close_date)
                CCAccount.account_id: "1",
                CCAccount.account_close_date: datetime(year=2024, month=12, day=9).date(),
                CCAccount.charge_off_date: datetime(year=2024, month=12, day=10).date(),
            },
            {
                # Null account_close_date, valid charge_off_date (should use charge_off_date)
                CCAccount.account_id: "2",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: datetime(year=2024, month=12, day=8).date(),
            },
            {
                # Blank account_close_date, null charge_off_date (should use DEFAULT_ERROR_DATE)
                CCAccount.account_id: "3",
                CCAccount.account_close_date: "",
                CCAccount.charge_off_date: None,
            },
            {
                # Both null (should use DEFAULT_ERROR_DATE)
                CCAccount.account_id: "4",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: None,
            },
        ]
    )

    # Run transformation
    result_df = date_closed(data)

    # Expected output
    expected_data = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {
                BaseSegment.account_id: "1",
                BaseSegment.date_closed: datetime(year=2024, month=12, day=9).date(),
            },
            {
                BaseSegment.account_id: "2",
                BaseSegment.date_closed: datetime(year=2024, month=12, day=8).date(),
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
    ).select(BaseSegment.account_id, BaseSegment.date_closed)

    # Assertion
    assert_df_equality(result_df, expected_data, ignore_row_order=True)
