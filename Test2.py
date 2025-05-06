
from pyspark.sql.functions import col, when, round
from pyspark.sql import DataFrame

def calculate_credit_limit_spark(ebcr_df: DataFrame, account_df: DataFrame) -> DataFrame:
    try:
        # Step 1: Left join on account_id
        joined_df = (
            ebcr_df.alias("ebcr")
            .join(account_df.alias("acc"), on=col("ebcr.account_id") == col("acc.account_id"), how="left")
        )

        # Step 2: Calculate credit_limit using business rules
        result_df = (
            joined_df.withColumn(
                "credit_limit",
                when(col("portfolio_type") == "O", 0)
                .when(col("portfolio_type") == "R", round(col("available_spending_amount")))
                .otherwise(None)
            )
        )

        return result_df.select("account_id", "portfolio_type", "available_spending_amount", "credit_limit")

    except Exception as e:
        print("Error in calculate_credit_limit_spark:", str(e))
        raise
