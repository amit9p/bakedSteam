
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def get_reportable_accounts(
    calculator_df: DataFrame,
    reporting_override_df: DataFrame,
    previously_reported_accounts_df: DataFrame
) -> DataFrame:

    # 🔹 Standardize account_id as String everywhere
    calculator_df = calculator_df.withColumn(
        EcbrCalculatorDfOutput.account_id.str,
        F.col(EcbrCalculatorDfOutput.account_id.str).cast(StringType())
    )

    reporting_override_df = reporting_override_df.withColumn(
        EcbrDFSOverride.account_id.str,
        F.col(EcbrDFSOverride.account_id.str).cast(StringType())
    )

    previously_reported_accounts_df = previously_reported_accounts_df.withColumn(
        EcbrCalculatorDfOutput.account_id.str,
        F.col(EcbrCalculatorDfOutput.account_id.str).cast(StringType())
    )

    # 1. Filter reporting_override to only "R" or "r"
    overrides = (
        reporting_override_df
        .filter(F.lower(F.col(EcbrDFSOverride.reporting_status.str)) == "r")
        .dropDuplicates([EcbrDFSOverride.account_id.str])
    )

    # 2. Eligible calculator rows (inner join with overrides)
    eligible_calc = calculator_df.join(
        overrides, on=EcbrCalculatorDfOutput.account_id.str, how="inner"
    )

    # 3. Remove already reported accounts (left anti join)
    final_df = eligible_calc.join(
        previously_reported_accounts_df,
        on=EcbrCalculatorDfOutput.account_id.str,
        how="left_anti"
    )

    # 4. Return only calculator fields
    return final_df.select([F.col(c) for c in calculator_df.columns])
