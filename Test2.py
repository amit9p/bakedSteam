

import pytest
from pyspark.sql import SparkSession
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ab.passthrough import get_current_credit_limit, get_original_credit_limit
from tests.helpers import create_partially_filled_dataset, assert_df_equality


@pytest.mark.usefixtures("spark")
def test_get_current_credit_limit(spark: SparkSession):
    input_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {"account_id": "A1", "available_spending_amount": 500.6},
            {"account_id": "A2", "available_spending_amount": None},
        ]
    )

    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {"account_id": "A1", "current_credit_limit": 501},
            {"account_id": "A2", "current_credit_limit": None},
        ]
    ).select(ABSegment.account_id, ABSegment.current_credit_limit)

    result_df = get_current_credit_limit(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)


@pytest.mark.usefixtures("spark")
def test_get_original_credit_limit(spark: SparkSession):
    input_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {"account_id": "A1", "available_spending_amount": 999.49},
            {"account_id": "A2", "available_spending_amount": None},
        ]
    )

    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {"account_id": "A1", "original_credit_limit": 999},
            {"account_id": "A2", "original_credit_limit": None},
        ]
    ).select(ABSegment.account_id, ABSegment.original_credit_limit)

    result_df = get_original_credit_limit(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)
