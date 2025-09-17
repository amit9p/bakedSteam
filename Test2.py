
from pyspark.sql import DataFrame, functions as F

# your schema classes
from schemas.ecbr_calculator_dfs_output import EcbrCalculatorOutput
from pre_charged_off.schemas.reporting_override import EcbrDFSOverride

# pull column names from schemas (strings)
ACC_ID  = EcbrCalculatorOutput.account_id.str
STATUS  = EcbrDFSOverride.reporting_status.str

def get_reportable_accounts(
    calculator_df: DataFrame,
    reporting_override_df: DataFrame,
    previously_reported_accounts_df: DataFrame,
) -> DataFrame:
    # 1) Overrides where reporting_status == 'R'
    overrides_R = (
        reporting_override_df
        .filter(F.upper(F.col(STATUS)) == F.lit("R"))
        .select(F.col(ACC_ID))
        .dropna(subset=[ACC_ID])
        .dropDuplicates([ACC_ID])
    )

    # 2) Eligible calculator rows (inner join to overrides)
    eligible_calc = calculator_df.join(overrides_R, on=ACC_ID, how="inner")

    # 3) Remove accounts already reported (left_anti)
    prev_ids = (
        previously_reported_accounts_df
        .select(F.col(ACC_ID))
        .dropna(subset=[ACC_ID])
        .dropDuplicates([ACC_ID])
    )
    final_df = eligible_calc.join(prev_ids, on=ACC_ID, how="left_anti")

    # 4) Return only calculator columns
    return final_df.select([F.col(c) for c in calculator_df.columns])
