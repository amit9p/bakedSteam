

from datetime import datetime
from chispa import assert_df_equality
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.ecbr_calculations.fields.base.date_closed import date_closed
from ecbr_card_self_service.ecbr_calculations.constants import DEFAULT_ERROR_DATE
from typedspark import create_partially_filled_dataset

def test_date_closed(spark):
    input_data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {
                "account_id": "1",
                "account_close_date": datetime(2024, 12, 9).date(),  # valid
                "charge_off_date": datetime(2024, 12, 10).date(),
            },
            {
                "account_id": "2",
                "account_close_date": None,  # fallback to charge_off_date
                "charge_off_date": datetime(2024, 12, 8).date(),
            },
            {
                "account_id": "3",
                "account_close_date": None,  # both None → error default
                "charge_off_date": None,
            },
            {
                "account_id": "4",
                "account_close_date": "",  # blank → fallback to charge_off_date
                "charge_off_date": datetime(2024, 12, 7).date(),
            },
        ],
    )

    expected_data = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {
                "account_id": "1",
                "date_closed": datetime(2024, 12, 9).date(),
            },
            {
                "account_id": "2",
                "date_closed": datetime(2024, 12, 8).date(),
            },
            {
                "account_id": "3",
                "date_closed": DEFAULT_ERROR_DATE,
            },
            {
                "account_id": "4",
                "date_closed": datetime(2024, 12, 7).date(),
            },
        ],
    )

    result_df = date_closed(input_data)
    assert_df_equality(result_df, expected_data, ignore_row_order=True)
