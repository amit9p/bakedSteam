
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
    # (account_id, account_status, deceased)
    cases = [
        ("A100", constants.AccountStatus.DA.value, None),  # NULL -> ERROR INT
        ("A101", "DA", True),                              # -> 3
        ("A102", "11", False),                             # -> 0
        ("A103", "97", None),                              # NULL -> ERROR INT
        ("A104", "da", False),                             # DA -> 3
    ]

    # ----- status stub (typed keys) -----
    status_rows = [
        {BaseSegment.account_id: a, BaseSegment.account_status: s}
        for a, s, _ in cases
    ]
    status_stub = create_partially_filled_dataset(spark, BaseSegment, data=status_rows).select(
        F.col(BaseSegment.account_id.str).alias(ABSegment.account_id.str),
        F.col(BaseSegment.account_status.str),
    )
    mock_calc_status.return_value = status_stub

    # ----- customer stub (typed keys) -----
    cust_rows = [
        {CustomerInformation.account_id: a, CustomerInformation.is_account_holder_deceased: d}
        for a, _, d in cases
    ]
    cust_stub = create_partially_filled_dataset(spark, CustomerInformation, data=cust_rows).select(
        F.col(CustomerInformation.account_id.str).alias(ABSegment.account_id.str),
        F.col(CustomerInformation.is_account_holder_deceased.str),
    )

    # schema-correct empties for unused params
    empty = create_partially_filled_dataset(spark, BaseSegment, data=[])

    # ----- Act -----
    uut_df = calculate_acc_update_delete_ind(
        customer_information_df=cust_stub,
        account_df=empty,
        customer_df=empty,
        recoveries_df=empty,
        fraud_df=empty,
        generated_fields_df=empty,
        caps_df=empty,
    )

    # assert only on INT indicator
    result_df = uut_df.select(
        F.col(ABSegment.ab_update_ind.str).cast("int").alias(ABSegment.ab_update_ind.str)
    )

    # ----- Expected (INT) -----
    err = int(constants.DEFAULT_ERROR_INTEGER)
    three = int(constants.SbfeAccountUpdateDeleteIndicator.THREE.value)
    zero = int(constants.SbfeAccountUpdateDeleteIndicator.ZERO.value)

    expected_rows = []
    for _, status, deceased in cases:
        if deceased is None:
            val = err
        elif (status or "").strip().lower() == constants.AccountStatus.DA.value or deceased is True:
            val = three
        else:
            val = zero
        expected_rows.append({ABSegment.ab_update_ind: val})

    expected_df = create_partially_filled_dataset(spark, ABSegment, data=expected_rows).select(
        F.col(ABSegment.ab_update_ind.str).cast("int").alias(ABSegment.ab_update_ind.str)
    )

    # ----- Assert -----
    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
