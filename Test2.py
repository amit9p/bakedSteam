
def test_get_current_credit_limit(spark: SparkSession):
    input_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {"account_id": "A1", "available_spending_amount": 501},
            {"account_id": "A2", "available_spending_amount": None}
        ]
    )

    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {"account_id": "A1", "current_credit_limit": 501},
            {"account_id": "A2", "current_credit_limit": -999999999}
        ]
    ).select(ABSegment.account_id, ABSegment.current_credit_limit)

    result_df = get_current_credit_limit(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)



def test_get_original_credit_limit(spark: SparkSession):
    input_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {"account_id": "A1", "available_spending_amount": 999},
            {"account_id": "A2", "available_spending_amount": None}
        ]
    )

    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {"account_id": "A1", "original_credit_limit": 999},
            {"account_id": "A2", "original_credit_limit": -999999999}
        ]
    ).select(ABSegment.account_id, ABSegment.original_credit_limit)

    result_df = get_original_credit_limit(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)


############










def get_current_credit_limit(input_df: DataFrame) -> DataFrame:
    """
    Returns account_id and current_credit_limit (rounded to whole dollar)
    from the spending_limit field in ecbr_df.
    """
    result_df = input_df.withColumn(
        ABSegment.current_credit_limit.str,
        when(
            col(CCAccount.available_spending_amount.str).isNull(),
            -999999999
        ).otherwise(
            round(col(CCAccount.available_spending_amount.str))
        )
    )

    return result_df.select(
        ABSegment.account_id,
        ABSegment.current_credit_limit
    )


def test_get_current_credit_limit(spark: SparkSession):
    input_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {CCAccount.account_id.str: "A1", CCAccount.available_spending_amount.str: 501},
            {CCAccount.account_id.str: "A2", CCAccount.available_spending_amount.str: None}
        ]
    )

    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {ABSegment.account_id.str: "A1", ABSegment.current_credit_limit.str: 501},
            {ABSegment.account_id.str: "A2", ABSegment.current_credit_limit.str: -999999999}
        ]
    ).select(ABSegment.account_id, ABSegment.current_credit_limit)

    result_df = get_current_credit_limit(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)



def get_original_credit_limit(input_df: DataFrame) -> DataFrame:
    """
    Returns account_id and original_credit_limit (same as current_credit_limit),
    rounded to whole dollar as per AB Field 20 definition.
    """
    current_credit_df = get_current_credit_limit(input_df)

    result_df = current_credit_df.withColumn(
        ABSegment.original_credit_limit.str,
        col(ABSegment.current_credit_limit.str)
    )

    return result_df.select(
        ABSegment.account_id,
        ABSegment.original_credit_limit
    )



def test_get_original_credit_limit(spark: SparkSession):
    input_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {CCAccount.account_id.str: "A1", CCAccount.available_spending_amount.str: 999},
            {CCAccount.account_id.str: "A2", CCAccount.available_spending_amount.str: None}
        ]
    )

    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {ABSegment.account_id.str: "A1", ABSegment.original_credit_limit.str: 999},
            {ABSegment.account_id.str: "A2", ABSegment.original_credit_limit.str: -999999999}
        ]
    ).select(ABSegment.account_id, ABSegment.original_credit_limit)

    result_df = get_original_credit_limit(input_df)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)

