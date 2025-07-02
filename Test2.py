
import pytest
from pyspark.sql import Row, SparkSession
from chispa import assert_df_equality
from typedspark import create_partially_filled_dataset

from your_module import calculate_basis_for_account_closure  # Replace with your actual file/module
from ecbr_calculations.fields.constants import (
    SPECIAL_COMMENT_CODE_M,
    COMPLIANCE_CODES_FOR_V,
    BASIS_CLOSURE_X,
    BASIS_CLOSURE_V
)

@pytest.mark.parametrize("scenario", ["scc_m", "ccc_match", "no_match"])
def test_calculate_basis_for_account_closure(spark: SparkSession, scenario):
    account_id = "A1"

    # Input mocks (using create_partially_filled_dataset)
    account_df = spark.createDataFrame([Row(account_id=account_id)])
    recoveries_df = spark.createDataFrame([Row(account_id=account_id)])
    customer_df = spark.createDataFrame([Row(account_id=account_id)])
    case_df = spark.createDataFrame([Row(account_id=account_id)])
    ecbr_generated_fields_df = spark.createDataFrame([Row(account_id=account_id)])

    # Mock SCC and CCC based on scenario
    if scenario == "scc_m":
        special_comment_df = spark.createDataFrame([Row(account_id=account_id, special_comment_code=SPECIAL_COMMENT_CODE_M)])
        compliance_code_df = spark.createDataFrame([Row(account_id=account_id, compliance_condition_code="DUMMY")])
        expected_val = BASIS_CLOSURE_X

    elif scenario == "ccc_match":
        special_comment_df = spark.createDataFrame([Row(account_id=account_id, special_comment_code="XX")])
        compliance_code_df = spark.createDataFrame([Row(account_id=account_id, compliance_condition_code=COMPLIANCE_CODES_FOR_V[0])])
        expected_val = BASIS_CLOSURE_V

    else:  # no_match
        special_comment_df = spark.createDataFrame([Row(account_id=account_id, special_comment_code="XX")])
        compliance_code_df = spark.createDataFrame([Row(account_id=account_id, compliance_condition_code="ZZ")])
        expected_val = None

    # Patch logic functions to return mocks
    import ecbr_calculations.fields.special_comment_code
    import ecbr_calculations.fields.compliance_condition_code
    ecbr_calculations.fields.special_comment_code.calculate_special_comment_code = lambda *args: special_comment_df
    ecbr_calculations.fields.compliance_condition_code.calculate_compliance_condition_code = lambda *args: compliance_code_df

    # Run function
    result_df = calculate_basis_for_account_closure(
        account_df,
        recoveries_df,
        customer_df,
        case_df,
        ecbr_generated_fields_df
    )

    # Expected output
    expected_df = spark.createDataFrame([Row(account_id=account_id, basis_for_account_closure=expected_val)])

    # Assertion
    assert_df_equality(result_df, expected_df, ignore_nullable=True)


_______


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
