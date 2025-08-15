
# test_acc_update_delete_ind.py
from unittest.mock import patch
from chispa import assert_df_equality
from pyspark.sql import functions as F

from ecbr_card_self_service.ecbr_calculations import constants
from ecbr_card_self_service.schemas.base.segment_import import BaseSegment
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecbr_card_self_service.schemas.customer_information import CustomerInformation
from ecbr_card_self_service.utils.spark import create_partially_filled_dataset

from ecbr_card_self_service.ecbr_calculations.small_business.small_business_charged_off.fields.ab.acc_update_delete_ind import (
    calculate_acc_update_delete_ind,
)

def _make_status_stub(spark, cases):
    # cases: [(acct, status)]
    rows = [
        {BaseSegment.account_id.str: acct, BaseSegment.account_status.str: status}
        for acct, status in cases
    ]
    return create_partially_filled_dataset(spark, BaseSegment, data=rows).select(
        # ensure the join key name matches what UUT uses
        F.col(BaseSegment.account_id.str).alias(ABSegment.account_id.str),
        F.col(BaseSegment.account_status.str),
    )

def _make_customer_stub(spark, cases):
    # cases: [(acct, deceased_bool_or_none)]
    rows = [
        {
            CustomerInformation.account_id.str: acct,
            CustomerInformation.is_account_holder_deceased.str: deceased,
        }
        for acct, deceased in cases
    ]
    return create_partially_filled_dataset(spark, CustomerInformation, data=rows).select(
        F.col(CustomerInformation.account_id.str).alias(ABSegment.account_id.str),
        F.col(CustomerInformation.is_account_holder_deceased.str),
    )

@patch(
    "ecbr_card_self_service.ecbr_calculations.small_business.small_business_charged_off.fields.ab.acc_update_delete_ind.calculate_account_status"
)
def test_acc_update_delete_ind_rules(mock_calc_status, spark):
    # --------------- Arrange ---------------
    # (account_id, account_status, deceased)
    cases = [
        ("A100", constants.AccountStatus.DA.value, None),  # "da" -> 3
        ("A101", "DA", True),                              # uppercase -> 3 (deceased True also -> 3)
        ("A102", "11", False),                             # else -> 0
        ("A103", "97", None),                              # else -> 0
        ("A104", "da", False),                             # lowercase -> 3
    ]

    status_stub = _make_status_stub(spark, [(a, s) for a, s, _ in cases])
    mock_calc_status.return_value = status_stub

    cust_stub = _make_customer_stub(spark, [(a, d) for a, _, d in cases])

    # schema-correct empty inputs for unused params
    empty = create_partially_filled_dataset(spark, BaseSegment, data=[])

    # --------------- Act ---------------
    result_df = calculate_acc_update_delete_ind(
        customer_information_df=cust_stub,
        account_df=empty,
        recoveries_df=empty,
        fraud_df=empty,
        generated_fields_df=empty,
        caps_df=empty,
        abs_df=empty,
    ).select(ABSegment.account_id.str, ABSegment.ab_update_ind.str)

    # --------------- Expect ---------------
    def expected_ind(status, deceased):
        if (status or "").strip().lower() == constants.AccountStatus.DA.value:
            return constants.SbfeAccountUpdateDeleteIndicator.THREE.value
        if deceased is True:
            return constants.SbfeAccountUpdateDeleteIndicator.THREE.value
        return constants.SbfeAccountUpdateDeleteIndicator.ZERO.value

    expected_rows = [
        {ABSegment.account_id.str: a, ABSegment.ab_update_ind.str: expected_ind(s, d)}
        for a, s, d in cases
    ]

    expected_df = create_partially_filled_dataset(spark, ABSegment, data=expected_rows).select(
        ABSegment.account_id.str, ABSegment.ab_update_ind.str
    )

    # --------------- Assert ---------------
    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
