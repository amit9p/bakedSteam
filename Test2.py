
from ecbr_card_self_service.tests.helpers.dataset_utils import create_partially_filled_dataset  # adjust path if needed

def test_amount_charged_off_by_creditor_with_mocked_field23(spark):



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
