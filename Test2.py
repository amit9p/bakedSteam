
# test_acc_update_delete_ind.py
from unittest.mock import patch
from chispa import assert_df_equality
from pyspark.sql import functions as F
from typedspark import create_partially_filled_dataset

from ecbr_card_self_service.ecbr_calculations import constants
from ecbr_card_self_service.schemas.base_segment import BaseSegment          # adjust if your path differs
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecbr_card_self_service.schemas.customer_information import CustomerInformation
from ecbr_card_self_service.ecbr_calculations.small_business.small_business_charged_off.fields.ab.acc_update_delete_ind import (
    calculate_acc_update_delete_ind,
)

# ---------- helpers (typed-column keys, lists only) ----------

def _make_status_stub(spark, cases):
    """
    cases: list[tuple[str, str]] -> (account_id, account_status)
    Keys are TYPED columns (e.g., BaseSegment.account_id), not .str.
    """
    cases = list(cases)
    rows = [
        {BaseSegment.account_id: acct, BaseSegment.account_status: status}
        for acct, status in cases
    ]
    df = create_partially_filled_dataset(spark, BaseSegment, data=rows)
    # Ensure the join key name matches what the UUT uses
    return df.select(
        F.col(BaseSegment.account_id.str).alias(ABSegment.account_id.str),
        F.col(BaseSegment.account_status.str),
    )


def _make_customer_stub(spark, cases):
    """
    cases: list[tuple[str, Optional[bool]]] -> (account_id, deceased)
    """
    cases = list(cases)
    rows = [
        {
            CustomerInformation.account_id: acct,
            CustomerInformation.is_account_holder_deceased: deceased,
        }
        for acct, deceased in cases
    ]
    df = create_partially_filled_dataset(spark, CustomerInformation, data=rows)
    return df.select(
        F.col(CustomerInformation.account_id.str).alias(ABSegment.account_id.str),
        F.col(CustomerInformation.is_account_holder_deceased.str),
    )


# ---------- test (assert on INT only) ----------

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

    # Stubs
    status_stub = _make_status_stub(spark, [(a, s) for a, s, _ in cases])
    mock_calc_status.return_value = status_stub
    cust_stub = _make_customer_stub(spark, [(a, d) for a, _, d in cases])

    # schema-correct empties for unused inputs in the UUT
    empty = create_partially_filled_dataset(spark, BaseSegment, data=[])

    # ---- Act: call UUT ----
    uut_df = calculate_acc_update_delete_ind(
        customer_information_df=cust_stub,
        account_df=empty,
        customer_df=empty,           # match real signature
        recoveries_df=empty,
        fraud_df=empty,
        generated_fields_df=empty,
        caps_df=empty,
    )

    # We assert ONLY on the integer indicator
    result_df = uut_df.select(
        F.col(ABSegment.ab_update_ind.str).cast("int").alias(ABSegment.ab_update_ind.str)
    )

    # ---- Build expected (INT values) ----
    def expected_ind(status, deceased):
        if (status or "").strip().lower() == constants.AccountStatus.DA.value:
            return constants.SbfeAccountUpdateDeleteIndicator.THREE.value
        if deceased is True:
            return constants.SbfeAccountUpdateDeleteIndicator.THREE.value
        return constants.SbfeAccountUpdateDeleteIndicator.ZERO.value

    expected_rows = [
        {ABSegment.ab_update_ind: expected_ind(s, d)}  # typed key; value is int
        for _, s, d in cases
    ]
    expected_df = create_partially_filled_dataset(spark, ABSegment, data=expected_rows).select(
        F.col(ABSegment.ab_update_ind.str).cast("int").alias(ABSegment.ab_update_ind.str)
    )

    # ---- Assert (only one INT column in both DFs) ----
    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )
