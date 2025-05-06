

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, round as spark_round, lit
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.schemas.ecbr_generated_fields import ECBRGeneratedFields
from ecbr_card_self_service.ecbr_calculations.fields.base.portfolio_type import portfolio_type
from ecbr_card_self_service.ecbr_calculations.utils.null_utility import check_if_any_are_null

# Constants
PORTFOLIO_TYPE_O = "O"
PORTFOLIO_TYPE_R = "R"
CREDIT_LIMIT_ZERO = 0

def calculate_credit_limit_spark(ebcr_df: DataFrame, account_df: DataFrame) -> DataFrame:
    try:
        # Step 1: Get portfolio_type-enriched DataFrame
        portfolio_type_df = portfolio_type(ebcr_df)

        # Step 2: Join with account DataFrame on account_id
        joined_df = portfolio_type_df.join(
            account_df,
            on=col(ECBRGeneratedFields.account_id.str) == col(CCAccount.account_id.str),
            how="left"
        )

        # Step 3: Compute credit limit per business rules
        final_df = joined_df.withColumn(
            BaseSegment.credit_limit.str,
            when(
                check_if_any_are_null([BaseSegment.portfolio_type.str]), lit(None)
            ).when(
                col(BaseSegment.portfolio_type.str) == PORTFOLIO_TYPE_O, lit(CREDIT_LIMIT_ZERO)
            ).when(
                col(BaseSegment.portfolio_type.str) == PORTFOLIO_TYPE_R,
                spark_round(col(CCAccount.available_spending_amount.str))
            ).otherwise(lit(None))
        )

        # Step 4: Select final output columns
        return final_df.select(
            BaseSegment.account_id.str,
            BaseSegment.credit_limit.str
        )

    except Exception as e:
        print("Error in calculate_credit_limit_spark:", str(e))
        raise
