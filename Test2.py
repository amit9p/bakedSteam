
from pyspark.sql.functions import col, when, round
from pyspark.sql import DataFrame

def calculate_credit_limit_spark(ebcr_df: DataFrame, account_df: DataFrame) -> DataFrame:
    try:
        # Step 1: Derive portfolio_type_df from ebcr_df
        portfolio_type_df = portfolio_type(ebcr_df)

        # Step 2: Join portfolio_type_df with account_df on account_id
        joined_df = (
            portfolio_type_df.alias("pt")
            .join(account_df.alias("acc"),
                  on=col("pt.account_id") == col("acc.account_id"),
                  how="left")
        )

        # Step 3: Apply business rules to calculate credit_limit
        result_df = (
            joined_df.withColumn(
                BaseSegment.credit_limit.str,
                when(col("pt.portfolio_type") == "O", 0)
                .when(col("pt.portfolio_type") == "R", round(col("acc.available_spending_amount")))
                .otherwise(None)
            )
        )

        return result_df.select(
            BaseSegment.account_id.str,
            BaseSegment.credit_limit.str
        )

    except Exception as e:
        print("Error in calculate_credit_limit_spark:", str(e))
        raise
