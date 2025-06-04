

import pytest
from pyspark.sql import Row
from chispa import assert_df_equality
from amount_charged_off_by_creditor import amount_charged_off_by_creditor

def test_amount_charged_off_by_creditor(spark):
    # Input DataFrames
    ccaccount_df = spark.createDataFrame([
        Row(account_id="A1", posted_balance=150),
        Row(account_id="A2", posted_balance=0),
        Row(account_id="A3", posted_balance=300)
    ])

    customer_df = spark.createDataFrame([
        Row(account_id="A1"),
        Row(account_id="A2"),
        Row(account_id="A3")
    ])

    recoveries_df = spark.createDataFrame([
        Row(account_id="A1"),
        Row(account_id="A2"),
        Row(account_id="A3")
    ])

    misc_df = spark.createDataFrame([
        Row(account_id="A1"),
        Row(account_id="A2"),
        Row(account_id="A3")
    ])

    ecbr_generated_fields_df = spark.createDataFrame([
        Row(account_id="A1", account_status="97"),
        Row(account_id="A2", account_status="11"),
        Row(account_id="A3", account_status="64")
    ])

    # Expected output
    expected_df = spark.createDataFrame([
        Row(account_id="A1", amount_charged_off_by_creditor=150),
        Row(account_id="A2", amount_charged_off_by_creditor=0),
        Row(account_id="A3", amount_charged_off_by_creditor=300)
    ])

    # Function under test
    result_df = amount_charged_off_by_creditor(
        ccaccount_df,
        customer_df,
        recoveries_df,
        misc_df,
        ecbr_generated_fields_df
    )

    # Assertion
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)
