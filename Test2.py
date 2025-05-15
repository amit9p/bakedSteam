

def test_get_current_credit_limit(spark: SparkSession):
    ccaccount_data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {CCAccount.account_id: "A1", CCAccount.available_spending_amount: 501},
            {CCAccount.account_id: "A2", CCAccount.available_spending_amount: None},
        ],
    )

    result_df = get_current_credit_limit(ccaccount_data)

    expected_data = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {ABSegment.account_id: "A1", ABSegment.current_credit_limit: 501},
            {ABSegment.account_id: "A2", ABSegment.current_credit_limit: -999999999},
        ],
    ).select(ABSegment.account_id, ABSegment.current_credit_limit)

    assert_df_equality(result_df, expected_data, ignore_row_order=True, ignore_nullable=True)



def test_get_original_credit_limit(spark: SparkSession):
    ccaccount_data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {CCAccount.account_id: "A1", CCAccount.available_spending_amount: 999},
            {CCAccount.account_id: "A2", CCAccount.available_spending_amount: None},
        ],
    )

    result_df = get_original_credit_limit(ccaccount_data)

    expected_data = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {ABSegment.account_id: "A1", ABSegment.original_credit_limit: 999},
            {ABSegment.account_id: "A2", ABSegment.original_credit_limit: -999999999},
        ],
    ).select(ABSegment.account_id, ABSegment.original_credit_limit)

    assert_df_equality(result_df, expected_data, ignore_row_order=True, ignore_nullable=True)
