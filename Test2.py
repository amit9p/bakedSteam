
import pytest
from chispa import assert_df_equality
from unittest.mock import patch

from amount_charged_off_by_creditor import amount_charged_off_by_creditor

from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.schemas.customer_information import CustomerInformation
from ecbr_card_self_service.schemas.recoveries import Recoveries
from ecbr_card_self_service.schemas.ecbr_generated_fields import ECBRGeneratedFields
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment

from ecbr_card_self_service.tests.helpers.dataset_utils import create_partially_filled_dataset

def test_amount_charged_off_by_creditor_with_mocked_field23(spark):
    # ✅ Input DataFrames
    account_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {CCAccount.account_id: "1", CCAccount.posted_balance: 100},
            {CCAccount.account_id: "2", CCAccount.posted_balance: 200},
            {CCAccount.account_id: "3", CCAccount.posted_balance: 300},
        ]
    )

    customer_df = create_partially_filled_dataset(
        spark,
        CustomerInformation,
        data=[
            {CustomerInformation.account_id: "1"},
            {CustomerInformation.account_id: "2"},
            {CustomerInformation.account_id: "3"},
        ]
    )

    recoveries_df = create_partially_filled_dataset(
        spark,
        Recoveries,
        data=[
            {Recoveries.account_id: "1"},
            {Recoveries.account_id: "2"},
            {Recoveries.account_id: "3"},
        ]
    )

    misc_df = create_partially_filled_dataset(
        spark,
        CCAccount,  # Replace with correct misc schema if needed
        data=[
            {CCAccount.account_id: "1"},
            {CCAccount.account_id: "2"},
            {CCAccount.account_id: "3"},
        ]
    )

    ecbr_generated_fields_df = create_partially_filled_dataset(
        spark,
        ECBRGeneratedFields,
        data=[
            {ECBRGeneratedFields.account_id: "1", ECBRGeneratedFields.account_status: "97"},
            {ECBRGeneratedFields.account_id: "2", ECBRGeneratedFields.account_status: "64"},
            {ECBRGeneratedFields.account_id: "3", ECBRGeneratedFields.account_status: "11"},
        ]
    )

    # ✅ Mocked Field 23 (original_charge_off_amount) output
    mocked_field23_df = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {BaseSegment.account_id: "1", BaseSegment.original_charge_off_amount: 100},
            {BaseSegment.account_id: "2", BaseSegment.original_charge_off_amount: 200},
            {BaseSegment.account_id: "3", BaseSegment.original_charge_off_amount: 0},
        ]
    )

    # ✅ Expected Field 73 output
    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {ABSegment.account_id: "1", ABSegment.amount_charged_off_by_creditor: 100},
            {ABSegment.account_id: "2", ABSegment.amount_charged_off_by_creditor: 200},
            {ABSegment.account_id: "3", ABSegment.amount_charged_off_by_creditor: 0},
        ]
    )

    # ✅ Patch Field 23 method
    with patch(
        "ecbr_card_self_service.ecbr_calculations.fields.base.original_charge_off_amount.original_charge_off_amount"
    ) as mock_func:
        mock_func.return_value = mocked_field23_df

        # Execute
        result_df = amount_charged_off_by_creditor(
            account_df,
            customer_df,
            recoveries_df,
            misc_df,
            ecbr_generated_fields_df
        )

        # ✅ Validate
        assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)
