


from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, lower, trim, when, max as spark_max

def fraud_claimed_flag(incidents_df: DataFrame) -> DataFrame:
    fraud_row = (
        (lower(trim(IncidentService.incident_type)) == "fraud") &
        (lower(trim(IncidentService.incident_status)).isin("reopened", "referred", "in_progress"))
    )

    return (
        incidents_df
        .select(
            IncidentService.account_id.alias("account_id"),
            when(fraud_row, lit(True)).otherwise(lit(False)).alias("is_fraud_claimed_on_account"),
        )
        .groupBy("account_id")
        .agg(spark_max("is_fraud_claimed_on_account").alias("is_fraud_claimed_on_account"))
    )
