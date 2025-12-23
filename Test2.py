

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, lower, trim, when, max as spark_max

def fraud_claimed_flag(incidents_df: DataFrame) -> DataFrame:
    fraud_row = (
        (lower(trim(col(IncidentService.incident_type))) == "fraud") &
        (
            lower(trim(col(IncidentService.incident_status)))
            .isin("reopened", "referred", "in_progress")
        )
    )

    return (
        incidents_df
        .select(
            col(IncidentService.account_id).alias("account_id"),
            when(fraud_row, lit(True))
            .otherwise(lit(False))
            .alias("is_fraud_claimed_on_account"),
        )
        .groupBy("account_id")
        .agg(
            spark_max("is_fraud_claimed_on_account")
            .alias("is_fraud_claimed_on_account")
        )
    )
