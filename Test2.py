[![Calculator Architecture](/img/calculator-arch.png)](/img/calculator-arch.png)

# Calculator Architecture

Here’s the high-level design:

![Calculator Architecture](/img/diagram.png)


Option 2: Flatten Structs for Testing

If your test framework must use CSV:

Flatten the struct into multiple columns (struct.field1 → col1, struct.field2 → col2).

Store these as simple columns in CSV.


# test_acc_update_delete_ind.py
from unittest.mock import patch
from chispa import assert_df_equality
from pyspark.sql import functions as F
from typedspark import create_partially_filled_dataset

from ecbr_card_self_service.ecbr_calculations import constants
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecbr_card_self_service.schemas.customer_information import CustomerInformation
from ecbr_card_self_service.ecbr_calculations.small_business.small_business_charged_off.fields.ab.acc_update_delete_ind import (
    calculate_acc_update_delete_ind,
)

@patch(
    "ecbr_card_self_service.ecbr_calculations.small_business.small_business_charged_off.fields.ab.acc_update_delete_ind.calculate_account_status"
)
def test_acc_update_delete_ind_rules(mock_calc_status, spark):
    # ---------- Build the patched status DF (hardcoded rows) ----------
    status_df = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {BaseSegment.account_id: "A100", BaseSegment.account_status: constants.AccountStatus.DA.value},
            {BaseSegment.account_id: "A101", BaseSegment.account_status: "DA"},
            {BaseSegment.account_id: "A102", BaseSegment.account_status: "11"},
            {BaseSegment.account_id: "A103", BaseSegment.account_status: "97"},
            {BaseSegment.account_id: "A104", BaseSegment.account_status: "da"},
        ],
    ).select(
        F.col(BaseSegment.account_id.str).alias(ABSegment.account_id.str),
        F.col(BaseSegment.account_status.str),
    )
    mock_calc_status.return_value = status_df

    # ---------- Build the customer_information DF (hardcoded rows) ----------
    customer_information_df = create_partially_filled_dataset(
        spark,
        CustomerInformation,
        data=[
            {CustomerInformation.account_id: "A100", CustomerInformation.is_account_holder_deceased: None},
            {CustomerInformation.account_id: "A101", CustomerInformation.is_account_holder_deceased: True},
            {CustomerInformation.account_id: "A102", CustomerInformation.is_account_holder_deceased: False},
            {CustomerInformation.account_id: "A103", CustomerInformation.is_account_holder_deceased: None},
            {CustomerInformation.account_id: "A104", CustomerInformation.is_account_holder_deceased: False},
        ],
    ).select(
        F.col(CustomerInformation.account_id.str).alias(ABSegment.account_id.str),
        F.col(CustomerInformation.is_account_holder_deceased.str),
    )

    # schema-correct empties for unused inputs
    empty = create_partially_filled_dataset(spark, BaseSegment, data=[])

    # ---------- Act ----------
    result_df = calculate_acc_update_delete_ind(
        customer_information_df=customer_information_df,
        account_df=empty,
        customer_df=empty,
        recoveries_df=empty,
        fraud_df=empty,
        generated_fields_df=empty,
        caps_df=empty,
    ).select(
        ABSegment.account_id.str,
        F.col(ABSegment.ab_update_ind.str).cast("int").alias(ABSegment.ab_update_ind.str),
    )

    # ---------- Expected (hardcoded rows) ----------
    # Rules: NULL deceased -> DEFAULT_ERROR_INTEGER; DA/deceased -> 3; else -> 0
    expected_df = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {ABSegment.account_id: "A100", ABSegment.ab_update_ind: constants.DEFAULT_ERROR_INTEGER},              # NULL -> ERR
            {ABSegment.account_id: "A101", ABSegment.ab_update_ind: constants.SbfeAccountUpdateDeleteIndicator.THREE.value},  # True -> 3
            {ABSegment.account_id: "A102", ABSegment.ab_update_ind: constants.SbfeAccountUpdateDeleteIndicator.ZERO.value},   # else -> 0
            {ABSegment.account_id: "A103", ABSegment.ab_update_ind: constants.DEFAULT_ERROR_INTEGER},              # NULL -> ERR
            {ABSegment.account_id: "A104", ABSegment.ab_update_ind: constants.SbfeAccountUpdateDeleteIndicator.THREE.value},  # DA -> 3
        ],
    ).select(
        ABSegment.account_id.str,
        F.col(ABSegment.ab_update_ind.str).cast("int").alias(ABSegment.ab_update_ind.str),
    )

    # ---------- Assert ----------
    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
