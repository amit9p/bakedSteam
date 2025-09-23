# Grab only schema fields that actually exist in DF
calc_cols = [
    c for c in EcbrCalculatorOutput.__annotations__.keys()
    if c in actual_df.columns and c in expected_df.columns
]

print("Common columns being compared:", calc_cols)

assert_df_equality(
    actual_df.select(*calc_cols),
    expected_df.select(*calc_cols),
    ignore_row_order=True,
    ignore_column_order=True,
)

______<<<<



return final_df.select([F.col(c) for c in EcbrCalculatorOutput.__annotations__.keys()])

calc_cols = list(Calc.__annotations__.keys())
assert_df_equality(
    actual_df.select(*calc_cols),
    expected_df.select(*calc_cols),
    ignore_row_order=True,
    ignore_column_order=True,
)

calc_cols = list(CalcSchema.__annotations__.keys())

________


from pyspark.sql import DataFrame, functions as F
from .schemas.ecbr_dfs_account import EcbrCalculatorOutput  # import your schema class


ACC_ID = EcbrCalculatorOutput.account_id.str
STATUS = "reporting_status"


def get_reportable_accounts(
    calculator_df: DataFrame,
    reporting_override_df: DataFrame,
    previous_reported_accounts_df: DataFrame,
) -> DataFrame:
    # 1) Keep only overrides with status = 'R' (case-insensitive)
    overrides = (
        reporting_override_df
        .filter(F.upper(F.col(STATUS)) == F.lit("R"))
        .select(F.col(ACC_ID).alias(ACC_ID))
        .dropDuplicates([ACC_ID])
    )

    # 2) Eligible calculator rows (inner join to overrides)
    eligible_calc = calculator_df.join(overrides, on=ACC_ID, how="inner")

    # 3) Remove accounts already reported (left anti join)
    not_reported = eligible_calc.join(
        previous_reported_accounts_df.select(ACC_ID),
        on=ACC_ID,
        how="left_anti"
    )

    # 4) Always return only *schema-defined columns*
    calc_cols = list(EcbrCalculatorOutput.__annotations__.keys())
    return not_reported.select([F.col(c) for c in calc_cols])


calc_cols = list(EcbrCalculatorOutput.__annotations__.keys())
assert_df_equality(
    actual_df.select(*calc_cols),
    expected_df.select(*calc_cols),
    ignore_row_order=True,
    ignore_column_order=True,
)


