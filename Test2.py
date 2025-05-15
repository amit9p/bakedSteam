
def test_get_current_credit_limit():
    input_df = create_partially_filled_dataset(
        CCAccount,
        data=[
            {"account_id": "A1", "available_spending_amount": 501},
            {"account_id": "A2", "available_spending_amount": None},
        ]
    )

    expected_df = create_partially_filled_dataset(
        ABSegment,
        data=[
            {"account_id": "A1", "current_credit_limit": 501},
            {"account_id": "A2", "current_credit_limit": -999999999},
        ]
    ).select(ABSegment.account_id, ABSegment.current_credit_limit)

    result_df = get_current_credit_limit(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)
