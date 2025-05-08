
{BaseSegment.account_id: "1", BaseSegment.credit_limit: None},



from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, round as spark_round
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.ecbr_calculations.fields.base.portfolio_type import portfolio_type

CREDIT_LIMIT_ZERO = 0
PORTFOLIO_TYPE_O = "0"
PORTFOLIO_TYPE_R = "R"

def calculate_credit_limit_spark(account_df: DataFrame) -> DataFrame:
    portfolio_df = portfolio_type(account_df)

    joined_df = portfolio_df.alias("pt").join(
        account_df.alias("acc"),
        on=col("pt." + BaseSegment.account_id.str) == col("acc." + CCAccount.account_id.str),
        how="left"
    )

    rounded_limit = spark_round(col("acc." + CCAccount.available_spending_amount.str).cast("double")).cast("int")

    result_df = joined_df.withColumn(
        BaseSegment.credit_limit.str,
        when(col("pt." + BaseSegment.portfolio_type.str).isNull(), lit(None))
        .when(col("pt." + BaseSegment.portfolio_type.str) == PORTFOLIO_TYPE_O, lit(CREDIT_LIMIT_ZERO))
        .when(col("pt." + BaseSegment.portfolio_type.str) == PORTFOLIO_TYPE_R, rounded_limit)
        .otherwise(lit(None))
    )

    return result_df.select(col("pt." + BaseSegment.account_id.str), col(BaseSegment.credit_limit.str))






from chispa import assert_df_equality
from typespark import create_partially_filled_dataset
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.ecbr_calculations.fields.base.credit_limit_field import calculate_credit_limit_spark
from ecbr_card_self_service.ecbr_calculations.fields.base.portfolio_type import PORTFOLIO_TYPE_O, PORTFOLIO_TYPE_R
from ecbr_card_self_service.ecbr_calculations.fields.base.portfolio_type import SEGMENT_ID_SMALL_BUSINESS

def test_calculate_credit_limit_spark(spark):
    account_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {CCAccount.account_id: "1", CCAccount.segment_id: SEGMENT_ID_SMALL_BUSINESS, CCAccount.available_spending_amount: 123},
            {CCAccount.account_id: "2", CCAccount.segment_id: "other_segment", CCAccount.available_spending_amount: 457},
            {CCAccount.account_id: "3", CCAccount.segment_id: None, CCAccount.available_spending_amount: 789}
        ]
    )

    expected_df = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {BaseSegment.account_id: "1", BaseSegment.credit_limit: 0},
            {BaseSegment.account_id: "2", BaseSegment.credit_limit: 457},
            {BaseSegment.account_id: "3", BaseSegment.credit_limit: 789}
        ]
    ).select(BaseSegment.account_id, BaseSegment.credit_limit)

    result_df = calculate_credit_limit_spark(account_df)

    assert_df_equality(expected_df, result_df, ignore_nullable=True)
