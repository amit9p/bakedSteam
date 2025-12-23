

from enum import Enum


class IncidentType(Enum):
    FRAUD = "FRAUD"
    DISPUTE = "DISPUTE"
    COMPLAINT = "COMPLAINT"


class IncidentStatus(Enum):
    REOPENED = "REOPENED"
    REFERRED = "REFERRED"
    IN_PROGRESS = "IN_PROGRESS"
    RESOLVED = "RESOLVED"




from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, lower, trim, when, max as spark_max

def fraud_claimed_flag(incidents_df: DataFrame) -> DataFrame:
    fraud_row = (
        (lower(trim(IncidentService.incident_type)) == constants.IncidentType.FRAUD.value.lower())
        & (
            lower(trim(IncidentService.incident_status)).isin(
                constants.IncidentStatus.REOPENED.value.lower(),
                constants.IncidentStatus.REFERRED.value.lower(),
                constants.IncidentStatus.IN_PROGRESS.value.lower(),
            )
        )
    )

    fraud_flag_col = when(fraud_row, lit(True)).otherwise(lit(False))

    return (
        incidents_df
        .select(
            IncidentService.account_id,
            fraud_flag_col.alias(Fraud.is_fraud_claimed_on_account.name),
        )
        .groupBy(IncidentService.account_id)
        .agg(
            spark_max(Fraud.is_fraud_claimed_on_account.name).alias(Fraud.is_fraud_claimed_on_account.name)
        )
    )
