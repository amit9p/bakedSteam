
from datetime import date
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecbr_card_self_service.schemas.sbfe.cc_account import CCAccount
from ecbr_card_self_service.ecbr_calculations.fields.ab.date_account_was_originally_opened import (
    date_account_was_originally_opened,
)
from typespark import create_partially_filled_dataset

def test_date_account_was_originally_opened(spark: SparkSession):
    # Input DataFrame simulating various edge cases
    ccaccount_data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            # Valid date
            {
                "account_id": "1",
                "account_open_date": date(2021, 1, 10),
            },
            # None date
            {
                "account_id": "2",
                "account_open_date": None,
            },
            # All valid dates
            {
                "account_id": "3",
                "account_open_date": date(2022, 5, 17),
            },
            # Edge case: earliest date
            {
                "account_id": "4",
                "account_open_date": date(1900, 1, 1),
            },
            # Edge case: all nulls
            {
                "account_id": "5",
                "account_open_date": None,
            },
        ],
    )

    # Expected DataFrame
    expected_data = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {
                "account_id": "1",
                "date_account_was_originally_opened": date(2021, 1, 10),
            },
            {
                "account_id": "2",
                "date_account_was_originally_opened": None,
            },
            {
                "account_id": "3",
                "date_account_was_originally_opened": date(2022, 5, 17),
            },
            {
                "account_id": "4",
                "date_account_was_originally_opened": date(1900, 1, 1),
            },
            {
                "account_id": "5",
                "date_account_was_originally_opened": None,
            },
        ],
    ).select(
        ABSegment.account_id,
        ABSegment.date_account_was_originally_opened,
    )

    # Call function
    result_df = date_account_was_originally_opened(ccaccount_data)

    # Assert DataFrames match
    assert_df_equality(expected_data, result_df, ignore_nullable=True)
