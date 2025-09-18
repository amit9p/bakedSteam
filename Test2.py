
from pyspark.sql import DataFrame, functions as F

# schema accessors (string paths to columns)
from ecbr_tenant_card_dfs_il_self_service.schemas.ecbr_calculator_dfs_output import EcbrCalculatorDfOutput
from ecbr_tenant_card_dfs_il_self_service.schemas.reporting_override import EcbrDFSOverride


def get_reportable_accounts(
    calculator_df: DataFrame,
    reporting_override_df: DataFrame,
    previously_reported_accounts_df: DataFrame,
) -> DataFrame:
    """
    Rules:
      1) Keep calculator rows whose account_id appears in reporting_override with reporting_status 'R' (case-insensitive)
      2) From those, remove any account_id already present in previously_reported_accounts
      3) Output only the calculator_df columns
    """

    # --- Column names via schemas (no hardcoding) ----------------------------
    ACC_ID_CALC = EcbrCalculatorDfOutput.account_id.str
    ACC_ID_OVR  = EcbrDFSOverride.account_id.str
    STATUS      = EcbrDFSOverride.reporting_status.str

    # --- Normalize IDs as strings once to avoid type/precision mismatches ----
    c = (
        calculator_df
        .withColumn("_acc_id_str", F.col(ACC_ID_CALC).cast("string"))
        .alias("c")
    )

    # eligible override accounts: reporting_status == 'r' (case-insensitive)
    o_keys = (
        reporting_override_df
        .select(
            F.col(ACC_ID_OVR).cast("string").alias("ovr_account_id"),
            F.lower(F.col(STATUS)).alias("ovr_status")
        )
        .filter(F.col("ovr_status") == F.lit("r"))
        .select("ovr_account_id")
        .dropna()
        .dropDuplicates()
        .alias("o")
    )

    # previously reported account ids
    p_keys = (
        previously_reported_accounts_df
        .select(F.col(ACC_ID_CALC).cast("string").alias("prev_account_id"))
        .dropna()
        .dropDuplicates()
        .alias("p")
    )

    # 1) inner join calculator with eligible overrides
    eligible_calc = c.join(F.broadcast(o_keys), F.col("c._acc_id_str") == F.col("o.ovr_account_id"), how="inner")

    # 2) left_anti to remove already-reported accounts
    pruned = eligible_calc.join(p_keys, F.col("c._acc_id_str") == F.col("p.prev_account_id"), how="left_anti")

    # 3) return only the original calculator columns (in original order)
    calc_cols = calculator_df.columns
    final_df = pruned.select(*[F.col(f"c.`{col}`") for col in calc_cols])

    return final_df
