
Based on the requirement in Image 1, we don't need to check if charge_off_date is null. The spec only states to use account_close_date if available, and if not, fall back to charge_off_date. Could you please remove the when(check_if_any_are_null(CCAccount.charge_off_date), lit(None))




check_if_any_are_null(CCAccount.charge_off_date) at the start doesn't make sense because we're not checking a list of columns, and it's likely returning True/False incorrectly. This may set everything to None unnecessarily.

Fix suggestion: You can remove that first when() entirely and simplify:

result_df = df.withColumn(
    sbfeABSegment.date_closed.str,
    when(CCAccount.account_close_date.isNull(), CCAccount.charge_off_date)
    .otherwise(CCAccount.account_close_date)
)



Excited to share a special moment with you all — we’ve welcomed a new member to our family!
[Baby’s name if you want to include it] arrived recently, and we’re filled with love, joy, and sleepless nights already.
Here’s a little glimpse of our happiness!



current approach is good and safer, especially because:

It handles both string and numeric inputs with decimal values.

.cast("double").cast("int") ensures Spark does a proper numeric conversion first, then safely rounds/truncates to integer.


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
