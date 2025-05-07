
from pyspark.sql.functions import col, when, lit, round as spark_round
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.schemas.ecbr_generated_fields import ECBRGeneratedFields
from ecbr_card_self_service.ecbr_calculations.fields.base.portfolio_type import portfolio_type
from ecbr_card_self_service.ecbr_calculations.utils.null_utility import check_if_any_are_null

# Constants (assumed to be defined in the same module)
PORTFOLIO_TYPE_O = "O"
PORTFOLIO_TYPE_R = "R"
CREDIT_LIMIT_ZERO = 0

def calculate_credit_limit_spark(ebcr_df: DataFrame, account_df: DataFrame) -> DataFrame:
    try:
        # Step 1: Get portfolio type from ebcr
        portfolio_type_df = portfolio_type(ebcr_df)

        # Step 2: Join with account df on account_id using aliases
        joined_df = (
            portfolio_type_df.alias("pt")
            .join(account_df.alias("acc"),
                  on=col("pt." + BaseSegment.account_id.str) == col("acc." + CCAccount.account_id.str),
                  how="left")
        )

        # Step 3: Round the spending limit
        rounded_assigned_limit = spark_round(
            col(f"acc.{CCAccount.available_spending_amount.str}").cast("double")
        ).cast("int")

        # Step 4: Apply business logic for credit limit
        result_df = joined_df.withColumn(
            BaseSegment.credit_limit.str,
            when(
                check_if_any_are_null(col("pt." + BaseSegment.portfolio_type.str)),
                lit(None)
            )
            .when(
                col("pt." + BaseSegment.portfolio_type.str) == PORTFOLIO_TYPE_O,
                lit(CREDIT_LIMIT_ZERO)
            )
            .when(
                col("pt." + BaseSegment.portfolio_type.str) == PORTFOLIO_TYPE_R,
                rounded_assigned_limit
            )
            .otherwise(lit(None))
        )

        return result_df.select(
            col("pt." + BaseSegment.account_id.str),
            col(BaseSegment.credit_limit.str)
        )

    except Exception as e:
        print("Error in calculate_credit_limit_spark:", str(e))
        raise
