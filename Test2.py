
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StringType

# Schema-driven names (string accessors you mentioned)
ACC_ID   = EcbrCalculatorDfOutput.account_id.str
INST     = EcbrCalculatorDfOutput.instnc_id.str           # include if needed
OVR_ID   = EcbrDFSOverride.account_id.str
OVR_INST = EcbrDFSOverride.instnc_id.str
STATUS   = EcbrDFSOverride.reporting_status.str
PRV_ID   = EcbrCalculatorDfOutput.account_id.str          # previous has same name

def get_reportable_accounts(
    calculator_df: DataFrame,
    reporting_override_df: DataFrame,
    previously_reported_accounts_df: DataFrame
) -> DataFrame:

    # normalize keys & status
    def n(col): return F.trim(F.col(col).cast(StringType()))
    c = (calculator_df
         .withColumn(ACC_ID, n(ACC_ID))
         .withColumn(INST,   n(INST)))
    o = (reporting_override_df
         .withColumn(OVR_ID, n(OVR_ID))
         .withColumn(OVR_INST, n(OVR_INST))
         .withColumn(STATUS, F.lower(F.trim(F.col(STATUS)))))
    p = (previously_reported_accounts_df
         .withColumn(PRV_ID, n(PRV_ID))
         .withColumn(INST,   n(INST)))

    # keep only R/r overrides and only the keys we need from overrides
    o_keys = (o.filter(F.col(STATUS) == "r")
                .select(F.col(OVR_ID).alias("o_account_id"),
                        F.col(OVR_INST).alias("o_instnc_id"))
                .dropna()
                .dropDuplicates())

    # eligible calculator rows: inner join with overrides (by id and instance if applicable)
    eligible_calc = (c.alias("c")
                     .join(o_keys.alias("o"),
                           (F.col("c."+ACC_ID)  == F.col("o.o_account_id")) &
                           (F.col("c."+INST)    == F.col("o.o_instnc_id")),   # drop this line if instance isn’t needed
                           "inner"))

    # de-dup the key so the anti-join keys are unique on the right
    p_keys = (p.select(F.col(PRV_ID).alias("p_account_id"),
                       F.col(INST).alias("p_instnc_id"))
                .dropna()
                .dropDuplicates())

    # left_anti: remove already reported
    final_df = (eligible_calc.alias("ec")
                .join(p_keys.alias("p"),
                      (F.col("ec."+ACC_ID) == F.col("p.p_account_id")) &
                      (F.col("ec."+INST)   == F.col("p.p_instnc_id")),   # drop if instance isn’t part of the key
                      "left_anti")
                .select("c.*"))  # <-- disambiguates: return only calculator columns

    return final_df
