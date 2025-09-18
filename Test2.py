from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StringType

# Short aliases
ACC_ID  = EcbrCalculatorDfOutput.account_id.str
OVR_ID  = EcbrDFSOverride.account_id.str
PRV_ID  = EcbrCalculatorDfOutput.account_id.str   # previous df also uses account_id

STATUS  = EcbrDFSOverride.reporting_status.str

def get_reportable_accounts(
    calculator_df: DataFrame,
    reporting_override_df: DataFrame,
    previously_reported_accounts_df: DataFrame
) -> DataFrame:

    # --- 0) Normalize join keys & status --------------------------------------
    def norm_id(colname):       # cast to string and trim
        return F.trim(F.col(colname).cast(StringType()))

    calculator_df = calculator_df.withColumn(ACC_ID, norm_id(ACC_ID))
    reporting_override_df = reporting_override_df.withColumn(OVR_ID, norm_id(OVR_ID)) \
                                                 .withColumn(STATUS, F.lower(F.trim(F.col(STATUS))))
    previously_reported_accounts_df = previously_reported_accounts_df.withColumn(PRV_ID, norm_id(PRV_ID))

    # --- 1) Filter overrides to only "r" (case/space proof) -------------------
    overrides = (reporting_override_df
                 .filter(F.col(STATUS) == "r")
                 .select(OVR_ID)          # only the key is needed for the join
                 .dropna(subset=[OVR_ID])
                 .dropDuplicates([OVR_ID]))

    # --- Diagnostics: counts & small samples ----------------------------------
    print("calc total:", calculator_df.count())
    print("overrides total:", overrides.count())
    print("prev total:", previously_reported_accounts_df.count())

    # Do we even have intersecting IDs?
    calc_has_override = calculator_df.join(overrides, calculator_df[ACC_ID] == overrides[OVR_ID], "left_semi")
    print("calc ∩ overrides:", calc_has_override.count())
    calc_not_in_prev  = calculator_df.join(previously_reported_accounts_df,
                                           ACC_ID, "left_anti")
    print("calc ¬ prev:", calc_not_in_prev.count())

    # --- 2) Eligible calculator rows (must have an 'R' override) --------------
    eligible_calc = calculator_df.join(overrides,
                                       calculator_df[ACC_ID] == overrides[OVR_ID],
                                       "inner")

    print("eligible_calc:", eligible_calc.count())

    # --- 3) Remove already reported accounts ----------------------------------
    final_df = eligible_calc.join(previously_reported_accounts_df,
                                  ACC_ID, "left_anti")

    print("final_df:", final_df.count())

    # --- 4) Return only calculator fields -------------------------------------
    return final_df.select([F.col(c) for c in calculator_df.columns])
