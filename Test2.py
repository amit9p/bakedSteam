

fraud_flag_df = fraud_claimed_flag(incidents_df) \
    .withColumnRenamed("is_fraud_claimed_on_account", "fraud_claimed_flag")

joined_df = joined_df.join(fraud_flag_df, on=BaseSegment.account_id.str, how="left")

joined_df = joined_df.fillna({"fraud_claimed_flag": False})

fraud_investigation_notification_true = (
    col("fraud_claimed_flag").cast("boolean") == True
)

