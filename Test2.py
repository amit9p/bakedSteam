
final_credit_limit = (
    when(
        check_if_any_are_null(col(BaseSegment.portfolio_type.str)),
        lit(None)
    )
    .when(col(BaseSegment.portfolio_type.str) == PORTFOLIO_TYPE_O, lit(CREDIT_LIMIT_ZERO))
    .when(col(BaseSegment.portfolio_type.str) == PORTFOLIO_TYPE_R, rounded_assigned_limit)
    .otherwise(lit(None))
)






check_if_any_are_null(col(BaseSegment.portfolio_type.str))


check_if_any_are_null([col(BaseSegment.portfolio_type.str)], lit(None))




joined_df = portfolio_type_df.join(
    account_df,
    on=portfolio_type_df[BaseSegment.account_id.str] == account_df[CCAccount.account_id.str],
    how="left"
)





from chispa import assert_df_equality
from typespark import create_partially_filled_dataset

from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.ecbr_calculations.fields.base.credit_limit_field import calculate_credit_limit_spark
from ecbr_card_self_service.ecbr_calculations.fields.base.portfolio_type import (
    PORTFOLIO_TYPE_O, PORTFOLIO_TYPE_R, SEGMENT_ID_SMALL_BUSINESS
)

def test_calculate_credit_limit_spark(spark):
    # Input for ebcr_df → will be passed into portfolio_type()
    ebcr_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {CCAccount.account_id: "1", CCAccount.segment_id: SEGMENT_ID_SMALL_BUSINESS},
            {CCAccount.account_id: "2", CCAccount.segment_id: "other_segment"},
            {CCAccount.account_id: "3", CCAccount.segment_id: None}
        ]
    )

    # Input for account_df
    account_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {CCAccount.account_id: "1", CCAccount.available_spending_amount: 123.45},
            {CCAccount.account_id: "2", CCAccount.available_spending_amount: 456.78},
            {CCAccount.account_id: "3", CCAccount.available_spending_amount: 789.12}
        ]
    )

    # Expected result
    expected_df = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {BaseSegment.account_id: "1", BaseSegment.credit_limit: 0},
            {BaseSegment.account_id: "2", BaseSegment.credit_limit: 457},
            {BaseSegment.account_id: "3", BaseSegment.credit_limit: 789}
        ]
    ).select(BaseSegment.account_id, BaseSegment.credit_limit)

    # Actual result
    result_df = calculate_credit_limit_spark(ebcr_df, account_df)

    assert_df_equality(result_df, expected_df, ignore_nullable=True)
