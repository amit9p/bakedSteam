
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
from ecbr_calculations.fields.constants import (
    SPECIAL_COMMENT_CODE_M,
    COMPLIANCE_CODES_FOR_V,
    BASIS_CLOSURE_X,
    BASIS_CLOSURE_V
)

def calculate_basis_for_account_closure(
    account_df: DataFrame,
    recoveries_df: DataFrame,
    customer_df: DataFrame,
    case_df: DataFrame,
    ecbr_generated_fields_df: DataFrame
) -> DataFrame:
    from ecbr_calculations.fields.special_comment_code import calculate_special_comment_code
    from ecbr_calculations.fields.compliance_condition_code import calculate_compliance_condition_code

    special_comment_df = calculate_special_comment_code(account_df, recoveries_df, customer_df)
    compliance_code_df = calculate_compliance_condition_code(account_df, case_df, ecbr_generated_fields_df)

    joined_df = special_comment_df.join(
        compliance_code_df,
        on="account_id",
        how="outer"
    )

    result_df = joined_df.withColumn(
        "basis_for_account_closure",
        when(col("special_comment_code") == SPECIAL_COMMENT_CODE_M, BASIS_CLOSURE_X)
        .when(col("compliance_condition_code").isin(*COMPLIANCE_CODES_FOR_V), BASIS_CLOSURE_V)
        .otherwise(None)
    ).select("account_id", "basis_for_account_closure")

    return result_df
