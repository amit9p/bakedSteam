
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
    # ✅ Input DataFrames with correct schema classes
    account_df = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {"account_id": "1", "posted_balance": 100},
            {"account_id": "2", "posted_balance": 200},
            {"account_id": "3", "posted_balance": 300},
        ]
    )

    customer_df = create_partially_filled_dataset(
        spark,
        CustomerInformation,
        data=[
            {"account_id": "1"},
            {"account_id": "2"},
            {"account_id": "3"},
        ]
    )

    recoveries_df = create_partially_filled_dataset(
        spark,
        Recoveries,
        data=[
            {"account_id": "1"},
            {"account_id": "2"},
            {"account_id": "3"},
        ]
    )

    misc_df = create_partially_filled_dataset(
        spark,
        CCAccount,  # Or correct misc schema if available
        data=[
            {"account_id": "1"},
            {"account_id": "2"},
            {"account_id": "3"},
        ]
    )

    ecbr_generated_fields_df = create_partially_filled_dataset(
        spark,
        ECBRGeneratedFields,
        data=[
            {"account_id": "1", "account_status": "97"},
            {"account_id": "2", "account_status": "64"},
            {"account_id": "3", "account_status": "11"},
        ]
    )

    # ✅ Mocked output of Field 23 (original_charge_off_amount)
    mocked_field23_df = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {"account_id": "1", "original_charge_off_amount": 100},
            {"account_id": "2", "original_charge_off_amount": 200},
            {"account_id": "3", "original_charge_off_amount": 0},
        ]
    )

    # ✅ Expected Field 73 output
    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {"account_id": "1", "amount_charged_off_by_creditor": 100},
            {"account_id": "2", "amount_charged_off_by_creditor": 200},
            {"account_id": "3", "amount_charged_off_by_creditor": 0},
        ]
    )

    # ✅ Patch original_charge_off_amount to return mocked Field 23 output
    with patch(
        "ecbr_card_self_service.ecbr_calculations.fields.base.original_charge_off_amount.original_charge_off_amount"
    ) as mock_func:
        mock_func.return_value = mocked_field23_df

        # Run the actual function
        result_df = amount_charged_off_by_creditor(
            account_df,
            customer_df,
            recoveries_df,
            misc_df,
            ecbr_generated_fields_df
        )

        # ✅ Assertion
        assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)
