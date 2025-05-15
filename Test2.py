
from ecb_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecb_card_self_service.schemas.cc_account import CCAccount
from ecb_card_self_service.ecbr_calculations.small_business.small_business_charged_off.fields.ab.passthrough import (
    get_current_credit_limit,
    get_original_credit_limit,
)
from tests.common_utils import create_partially_filled_dataset, assert_df_equality


def test_get_current_credit_limit():
    input_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {"account_id": "A1", "available_spending_amount": 501},
            {"account_id": "A2", "available_spending_amount": None},
        ]
    )

    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {"account_id": "A1", "current_credit_limit": 501},
            {"account_id": "A2", "current_credit_limit": -999999999},
        ]
    ).select(ABSegment.account_id, ABSegment.current_credit_limit)

    result_df = get_current_credit_limit(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)


def test_get_original_credit_limit():
    input_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {"account_id": "B1", "available_spending_amount": 999},
            {"account_id": "B2", "available_spending_amount": None},
        ]
    )

    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {"account_id": "B1", "original_credit_limit": 999},
            {"account_id": "B2", "original_credit_limit": -999999999},
        ]
    ).select(ABSegment.account_id, ABSegment.original_credit_limit)

    result_df = get_original_credit_limit(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)
