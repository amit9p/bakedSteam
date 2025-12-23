
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, lower, trim, max as spark_max, when

def fraud_claimed_flag(incidents_df: DataFrame) -> DataFrame:
    fraud_row = (
        (lower(trim(col("incident_type"))) == "fraud") &
        (lower(trim(col("incident_status"))).isin("reopened", "referred", "in_progress"))
    )

    return (
        incidents_df
        .select(
            col("account_id"),
            when(fraud_row, lit(True)).otherwise(lit(False)).alias("is_fraud_claimed_on_account")
        )
        .groupBy("account_id")
        .agg(spark_max("is_fraud_claimed_on_account").alias("is_fraud_claimed_on_account"))
    )
