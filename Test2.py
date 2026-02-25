from pyspark.sql.functions import when, to_date, current_date

result_df = input_df.withColumn(
    EcbrCalculatorOutput.formatted_date_of_account_information.str,
    when(
        account_status_13_or_64,
        to_date(ECBRCardDFSAccountsPrimary.transaction_date)
    ).otherwise(
        current_date()
    )
)
