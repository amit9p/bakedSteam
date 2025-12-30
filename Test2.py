expected_df = create_partially_filled_dataset(
    spark,
    ECBRGeneratedFields,
    data=[
        {
            ECBRGeneratedFields.account_id: 21,
            ECBRGeneratedFields.is_identity_fraud_claimed_on_account: True,
        },
        {
            ECBRGeneratedFields.account_id: 22,
            ECBRGeneratedFields.is_identity_fraud_claimed_on_account: False,
        },
    ],
).select(
    ECBRGeneratedFields.account_id.str,
    ECBRGeneratedFields.is_identity_fraud_claimed_on_account.str,
)

-----
fraud_flag_df = fraud_investigation_notification(incidents_df) \
    .select(
        BaseSegment.account_id_str,
        col(ECBRGeneratedFields.is_identity_fraud_claimed_on_account).alias("incidents_identity_fraud_flag")
    )


joined_df = joined_df.join(fraud_flag_df, on=BaseSegment.account_id_str, how="left")

joined_df = joined_df.withColumn(
    ECBRGeneratedFields.is_identity_fraud_claimed_on_account,
    F.coalesce(
        col("incidents_identity_fraud_flag"),
        col(ECBRGeneratedFields.is_identity_fraud_claimed_on_account),
        F.lit(False),
    )
).drop("incidents_identity_fraud_flag")



fraud_investigation_notification_true = (
    col(ECBRGeneratedFields.is_identity_fraud_claimed_on_account).cast("boolean") == True
)
