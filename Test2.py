
# test_acc_update_delete_ind.py
from unittest.mock import patch
from chispa import assert_df_equality
from pyspark.sql import functions as F

# adjust these imports to your project paths if needed
from typedspark import create_partially_filled_dataset
from ecbr_card_self_service.ecbr_calculations import constants
from ecbr_card_self_service.schemas.base_segment_import import BaseSegment
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecbr_card_self_service.schemas.customer_information import CustomerInformation
from ecbr_card_self_service.ecbr_calculations.small_business.small_business_charged_off.fields.ab.acc_update_delete_ind import (
    calculate_acc_update_delete_ind,
)

# ---------- helpers (defensive against generators/iterators) ----------

def _make_status_stub(spark, cases):
    # cases may be a generator; coerce to list
    cases = list(cases)
    rows = []
    for acct, status in cases:
        rows.append({
            BaseSegment.account_id.str: acct,
            BaseSegment.account_status.str: status,
        })
    # ensure each element is a dict (handles dict_items/dict_iterator just in case)
    rows = [dict(r) for r in rows]
    df = create_partially_filled_dataset(spark, BaseSegment, data=rows)
    return df.select(
        F.col(BaseSegment.account_id.str).alias(ABSegment.account_id.str),
        F.col(BaseSegment.account_status.str),
    )

def _make_customer_stub(spark, cases):
    cases = list(cases)
    rows = []
    for acct, deceased in cases:
        rows.append({
            CustomerInformation.account_id.str: acct,
            CustomerInformation.is_account_holder_deceased.str: deceased,
        })
    rows = [dict(r) for r in rows]
    df = create_partially_filled_dataset(spark, CustomerInformation, data=rows)
    return df.select(
        F.col(CustomerInformation.account_id.str).alias(ABSegment.account_id.str),
        F.col(CustomerInformation.is_account_holder_deceased.str),
    )

# ---------- test ----------

@patch(
    "ecbr_card_self_service.ecbr_calculations.small_business.small_business_charged_off.fields.ab.acc_update_delete_ind.calculate_account_status"
)
def test_acc_update_delete_ind_rules(mock_calc_status, spark):
    # (account_id, account_status, deceased)
    cases = [
        ("A100", constants.AccountStatus.DA.value, None),  # "da" -> 3
        ("A101", "DA", True),                              # uppercase -> 3 (also deceased True)
        ("A102", "11", False),                             # else -> 0
        ("A103", "97", None),                              # else -> 0
        ("A104", "da", False),                             # lowercase -> 3
    ]

    # build stubs
    status_stub = _make_status_stub(spark, [(a, s) for a, s, _ in cases])   # LIST, not generator
    mock_calc_status.return_value = status_stub
    cust_stub = _make_customer_stub(spark, [(a, d) for a, _, d in cases])    # LIST, not generator

    # schema-correct empties for unused inputs
    empty = create_partially_filled_dataset(spark, BaseSegment, data=[])

    # call UUT
    result_df = calculate_acc_update_delete_ind(
        customer_information_df=cust_stub,
        account_df=empty,
        customer_df=empty,          # match your real signature
        recoveries_df=empty,
        fraud_df=empty,
        generated_fields_df=empty,
        caps_df=empty,
    ).select(ABSegment.account_id.str, ABSegment.ab_update_ind.str)

    # expected
    def expected_ind(status, deceased):
        if (status or "").strip().lower() == constants.AccountStatus.DA.value:
            return constants.SbfeAccountUpdateDeleteIndicator.THREE.value
        if deceased is True:
            return constants.SbfeAccountUpdateDeleteIndicator.THREE.value
        return constants.SbfeAccountUpdateDeleteIndicator.ZERO.value

    expected_rows = [
        {ABSegment.account_id.str: a, ABSegment.ab_update_ind.str: expected_ind(s, d)}
        for a, s, d in list(cases)  # coerce to list for safety
    ]
    expected_rows = [dict(r) for r in expected_rows]
    expected_df = create_partially_filled_dataset(spark, ABSegment, data=expected_rows) \
        .select(ABSegment.account_id.str, ABSegment.ab_update_ind.str)

    # assert
    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
