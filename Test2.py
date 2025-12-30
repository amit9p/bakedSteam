
def fraud_investigation_notification(incidents_df: DataFrame) -> DataFrame:
    fraud_row = (
        lower(trim(col(IncidentService.incident_type))) == constants.IncidentType.FRAUD.value.lower()
    ) & (
        lower(trim(col(IncidentService.incident_status))).isin(
            constants.IncidentStatus.REOPENED.value.lower(),
            constants.IncidentStatus.REFERRED.value.lower(),
            constants.IncidentStatus.IN_PROGRESS.value.lower(),
        )
    )

    return (
        incidents_df
        .select(
            col(IncidentService.account_id).alias(BaseSegment.account_id_str),
            when(fraud_row, lit(True)).otherwise(lit(False)).alias(
                ECBRGeneratedFields.is_identity_fraud_claimed_on_account
            ),
        )
        .groupBy(BaseSegment.account_id_str)
        .agg(
            spark_max(ECBRGeneratedFields.is_identity_fraud_claimed_on_account).alias(
                ECBRGeneratedFields.is_identity_fraud_claimed_on_account
            )
        )
    )




if incidents_df is None:
    fraud_flag_df = account_df.select(BaseSegment.account_id_str).withColumn(
        ECBRGeneratedFields.is_identity_fraud_claimed_on_account,
        F.lit(False),
    )
else:
    fraud_flag_df = fraud_investigation_notification(incidents_df)




joined_df = joined_df.join(fraud_flag_df, on=BaseSegment.account_id_str, how="left")

joined_df = joined_df.fillna(
    {ECBRGeneratedFields.is_identity_fraud_claimed_on_account: False}
)



fraud_investigation_notification_true = (
    col(ECBRGeneratedFields.is_identity_fraud_claimed_on_account).cast("boolean") == True
)




--------------××××---------------

from behave import given

@given(r"the user data is available in the following datasets:?")



behave tests/ecbr_calculations/features -n "Process calculate_payment_rating from parquet file" -f plain --no-capture



behave -f plain --no-capture --no-capture-stderr -v tests/ecbr_calculations/features/consumer_features/base.feature -n "Process calculate_payment_rating from parquet file"




from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def calculate_account_status(
    account_df: DataFrame,
    customer_df: DataFrame,
    recoveries_df: DataFrame,
    fraud_df: DataFrame,
    generated_fields_df: DataFrame,
    caps_df: DataFrame,
    incidents_df: Optional[DataFrame] = None,   # ✅ optional
) -> DataFrame:


    if incidents_df is None:
    # default: no incident => no fraud claimed
    fraud_flag_df = account_df.select(BaseSegment.account_id.str).withColumn(
        "fraud_claimed_flag", F.lit(False)
    )
else:
    fraud_flag_df = (
        fraud_investigation_notification(incidents_df)
        .withColumnRenamed("is_fraud_claimed_on_account", "fraud_claimed_flag")
    )


joined_df = joined_df.join(fraud_flag_df, on=BaseSegment.account_id.str, how="left")
joined_df = joined_df.fillna({"fraud_claimed_flag": False})
