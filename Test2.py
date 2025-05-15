


import pytest
from pyspark.sql import SparkSession
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ab.passthrough import get_current_credit_limit, get_original_credit_limit
from tests.helpers import create_partially_filled_dataset, assert_df_equality


def test_get_current_credit_limit(spark: SparkSession):
    input_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {CCAccount.account_id: "A1", CCAccount.available_spending_amount: 500.6},
            {CCAccount.account_id: "A2", CCAccount.available_spending_amount: None},
        ]
    )

    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {ABSegment.account_id: "A1", ABSegment.current_credit_limit: 501},
            {ABSegment.account_id: "A2", ABSegment.current_credit_limit: None},
        ]
    ).select(ABSegment.account_id, ABSegment.current_credit_limit)

    result_df = get_current_credit_limit(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)


def test_get_original_credit_limit(spark: SparkSession):
    input_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {CCAccount.account_id: "A1", CCAccount.available_spending_amount: 999.49},
            {CCAccount.account_id: "A2", CCAccount.available_spending_amount: None},
        ]
    )

    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {ABSegment.account_id: "A1", ABSegment.original_credit_limit: 999},
            {ABSegment.account_id: "A2", ABSegment.original_credit_limit: None},
        ]
    ).select(ABSegment.account_id, ABSegment.original_credit_limit)

    result_df = get_original_credit_limit(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)
