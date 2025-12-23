
_______def test_is_fraud_claimed_on_account_helper(spark):
    # Input incidents: 2 accounts
    # - acct 21 has FRAUD + IN_PROGRESS => should be True
    # - acct 22 has FRAUD + RESOLVED    => should be False
    incidents_df = create_partially_filled_dataset(
        spark,
        IncidentsServiceIncident,  # use your actual schema/class for incidents df
        data=[
            {
                IncidentsServiceIncident.account_id: "21",
                IncidentsServiceIncident.incident_type: "FRAUD",
                IncidentsServiceIncident.incident_status: "IN_PROGRESS",
            },
            {
                IncidentsServiceIncident.account_id: "22",
                IncidentsServiceIncident.incident_type: "FRAUD",
                IncidentsServiceIncident.incident_status: "RESOLVED",
            },
        ],
    )

    # Expected output: exactly 2 columns from helper
    expected_df = create_partially_filled_dataset(
        spark,
        FraudClaimedOnAccount,  # create a tiny schema with account_id + is_fraud_claimed_on_account
        data=[
            {
                FraudClaimedOnAccount.account_id: "21",
                FraudClaimedOnAccount.is_fraud_claimed_on_account: True,
            },
            {
                FraudClaimedOnAccount.account_id: "22",
                FraudClaimedOnAccount.is_fraud_claimed_on_account: False,
            },
        ],
    )

    result_df = calculate_is_fraud_claimed_on_account(incidents_df)

    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )

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
