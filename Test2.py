
from pyspark.sql.functions import col
from pyspark.sql.types import *

df_converted = (
    df
    # Core columns
    .withColumn("account_id", col("account_id").cast(LongType()))
    .withColumn("credit_bureau_account_status", col("credit_bureau_account_status").cast(StringType()))
    .withColumn("compliance_condition_cd", col("compliance_condition_cd").cast(StringType()))
    .withColumn("special_comment", col("special_comment").cast(StringType()))
    .withColumn("payment_rating", col("payment_rating").cast(StringType()))
    .withColumn("portfolio_indicator", col("portfolio_indicator").cast(StringType()))
    .withColumn("portfolio_type_id", col("portfolio_type_id").cast(StringType()))
    .withColumn("pre_charge_off_date_last_reported", col("pre_charge_off_date_last_reported").cast(DateType()))
    .withColumn("comm_credit_bureau_account_status1", col("comm_credit_bureau_account_status1").cast(StringType()))
    .withColumn("comm_credit_bureau_account_status2", col("comm_credit_bureau_account_status2").cast(StringType()))
    .withColumn("comm_correction_indicator", col("comm_correction_indicator").cast(StringType()))
    .withColumn("account_closed_date", col("account_closed_date").cast(DateType()))
    .withColumn("date_first_delinquent", col("date_first_delinquent").cast(DateType()))
    .withColumn("credit_limit", col("credit_limit").cast(DoubleType()))
    .withColumn("reporting_status", col("reporting_status").cast(StringType()))
    .withColumn("reporting_reason", col("reporting_reason").cast(StringType()))
    .withColumn("comm_account_type", col("comm_account_type").cast(StringType()))
    .withColumn("date_last_reported", col("date_last_reported").cast(DateType()))
    .withColumn("identification_number", col("identification_number").cast(StringType()))
    .withColumn("account_type", col("account_type").cast(StringType()))
    .withColumn("high_balance", col("high_balance").cast(DoubleType()))
    .withColumn("terms_duration", col("terms_duration").cast(StringType()))
    .withColumn("terms_frequency", col("terms_frequency").cast(StringType()))
    .withColumn("delete_reason", col("delete_reason").cast(StringType()))
    .withColumn("past_due_indicator", col("past_due_indicator").cast(StringType()))
    .withColumn("comm_payment_rating", col("comm_payment_rating").cast(StringType()))
    
    # Member number fields (all string)
    .withColumn("experian_consumer_member_number1", col("experian_consumer_member_number1").cast(StringType()))
    .withColumn("experian_consumer_member_number2", col("experian_consumer_member_number2").cast(StringType()))
    .withColumn("equifax_consumer_member_number", col("equifax_consumer_member_number").cast(StringType()))
    .withColumn("transunion_consumer_member_number", col("transunion_consumer_member_number").cast(StringType()))
    .withColumn("montreal_of_canada_consumer_member_number", col("montreal_of_canada_consumer_member_number").cast(StringType()))
    .withColumn("hamilton_of_canada_consumer_member_number", col("hamilton_of_canada_consumer_member_number").cast(StringType()))
    .withColumn("puerto_rico_consumer_member_number", col("puerto_rico_consumer_member_number").cast(StringType()))
    .withColumn("dun_and_bradstreet_consumer_member_number", col("dun_and_bradstreet_consumer_member_number").cast(StringType()))
    .withColumn("northern_credit_consumer_member_number", col("northern_credit_consumer_member_number").cast(StringType()))
    .withColumn("experian_small_business_member_number", col("experian_small_business_member_number").cast(StringType()))
    .withColumn("equifax_small_business_member_number", col("equifax_small_business_member_number").cast(StringType()))
    .withColumn("dun_and_bradstreet_small_business_member_number", col("dun_and_bradstreet_small_business_member_number").cast(StringType()))
    .withColumn("financial_exchange_small_business_member_number", col("financial_exchange_small_business_member_number").cast(StringType()))

    # Dates and metadata
    .withColumn("snap_dt", col("snap_dt").cast(DateType()))
    .withColumn("edw_publn_id", col("edw_publn_id").cast(LongType()))
    .withColumn("instnc_id", col("instnc_id").cast(StringType()))
)
