
import pytest
from chispa import assert_df_equality
from unittest.mock import patch

from amount_charged_off_by_creditor import amount_charged_off_by_creditor
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.ab_segment import ABSegment

def test_amount_charged_off_by_creditor_with_mocked_field23(spark, create_partially_filled_dataset):
    # Input DataFrames
    account_df = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {BaseSegment.account_id.str: "1"},
            {BaseSegment.account_id.str: "2"},
            {BaseSegment.account_id.str: "3"},
        ]
    )

    customer_df = create_partially_filled_dataset(spark, BaseSegment, data=[
        {BaseSegment.account_id.str: "1"},
        {BaseSegment.account_id.str: "2"},
        {BaseSegment.account_id.str: "3"},
    ])

    recoveries_df = create_partially_filled_dataset(spark, BaseSegment, data=[
        {BaseSegment.account_id.str: "1"},
        {BaseSegment.account_id.str: "2"},
        {BaseSegment.account_id.str: "3"},
    ])

    misc_df = create_partially_filled_dataset(spark, BaseSegment, data=[
        {BaseSegment.account_id.str: "1"},
        {BaseSegment.account_id.str: "2"},
        {BaseSegment.account_id.str: "3"},
    ])

    ecbr_generated_fields_df = create_partially_filled_dataset(spark, BaseSegment, data=[
        {BaseSegment.account_id.str: "1"},
        {BaseSegment.account_id.str: "2"},
        {BaseSegment.account_id.str: "3"},
    ])

    # Mocked output of Field 23 (original_charge_off_amount)
    mocked_field23_df = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {
                BaseSegment.account_id.str: "1",
                BaseSegment.original_charge_off_amount.str: 100
            },
            {
                BaseSegment.account_id.str: "2",
                BaseSegment.original_charge_off_amount.str: 200
            },
            {
                BaseSegment.account_id.str: "3",
                BaseSegment.original_charge_off_amount.str: 0
            },
        ]
    )

    # Expected DataFrame
    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {
                ABSegment.account_id.str: "1",
                ABSegment.amount_charged_off_by_creditor.str: 100
            },
            {
                ABSegment.account_id.str: "2",
                ABSegment.amount_charged_off_by_creditor.str: 200
            },
            {
                ABSegment.account_id.str: "3",
                ABSegment.amount_charged_off_by_creditor.str: 0
            },
        ]
    )

    # Patch original_charge_off_amount
    with patch("ecbr_card_self_service.ecbr_calculations.fields.base.original_charge_off_amount.original_charge_off_amount") as mock_func:
        mock_func.return_value = mocked_field23_df

        # Call target method
        result_df = amount_charged_off_by_creditor(
            account_df,
            customer_df,
            recoveries_df,
            misc_df,
            ecbr_generated_fields_df
        )

        # Assert
        assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)



______________
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







from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from edq.generated_fields.segment import BaseSegment
from original_charge_off_amount import original_charge_off_amount

def amount_charged_off_by_creditor(
    account_df: DataFrame,
    customer_df: DataFrame,
    recoveries_df: DataFrame,
    misc_df: DataFrame,
    ecbr_generated_fields_df: DataFrame
) -> DataFrame:
    """
    This method calculates Field 73 by reusing the Field 23 logic.
    It returns a DataFrame with account_id and amount_charged_off_by_creditor.
    """
    field23_df = original_charge_off_amount(
        account_df,
        customer_df,
        recoveries_df,
        misc_df,
        ecbr_generated_fields_df
    )

    return field23_df.select(
        BaseSegment.account_id.str,
        BaseSegment.original_charge_off_amount.str.alias("amount_charged_off_by_creditor")
    )


import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col
from amount_charged_off_by_creditor import amount_charged_off_by_creditor

def test_amount_charged_off_by_creditor(spark):
    # Sample input DataFrames
    account_df = spark.createDataFrame([
        Row(account_id="A1", posted_balance=100),
        Row(account_id="A2", posted_balance=200),
    ])

    customer_df = spark.createDataFrame([Row(account_id="A1"), Row(account_id="A2")])
    recoveries_df = spark.createDataFrame([Row(account_id="A1"), Row(account_id="A2")])
    misc_df = spark.createDataFrame([Row(account_id="A1"), Row(account_id="A2")])
    ecbr_generated_fields_df = spark.createDataFrame([
        Row(account_id="A1", account_status="97"),
        Row(account_id="A2", account_status="64")
    ])

    # Expected DataFrame
    expected_df = spark.createDataFrame([
        Row(account_id="A1", amount_charged_off_by_creditor=100),
        Row(account_id="A2", amount_charged_off_by_creditor=200),
    ])

    # Call function
    result_df = amount_charged_off_by_creditor(
        account_df, customer_df, recoveries_df, misc_df, ecbr_generated_fields_df
    )

    # Assert
    assert result_df.collect() == expected_df.collect()
