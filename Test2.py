
from pyspark.sql import DataFrame, functions as F
from ecbr_card_self_service.ecbr_calculations.fields.base.account_status import calculate_account_status
from ecbr_card_self_service.ecbr_calculations import constants
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecbr_card_self_service.schemas.customer_information import CustomerInformation

def calculate_acc_update_delete_ind(
    customer_information_df: DataFrame,
    account_df: DataFrame,
    customer_df: DataFrame,
    recoveries_df: DataFrame,
    fraud_df: DataFrame,
    generated_fields_df: DataFrame,
    caps_df: DataFrame,
) -> DataFrame:
    # 1) derive account status
    account_status_df = calculate_account_status(
        account_df, customer_df, recoveries_df, fraud_df, generated_fields_df, caps_df
    )

    # 2) join to get deceased flag alongside status
    joined = account_status_df.join(
        customer_information_df, on=ABSegment.account_id.str, how="left"
    )

    # 3) conditions
    status_col   = F.trim(F.col(BaseSegment.account_status.str).cast("string"))
    status_is_DA = F.upper(status_col) == F.upper(F.lit(constants.AccountStatus.DA.value))  # "da"/"DA"

    dec_col           = F.col(CustomerInformation.is_account_holder_deceased.str).cast("boolean")
    deceased_is_null  = dec_col.isNull()
    deceased_is_true  = F.coalesce(dec_col, F.lit(False))

    # 4) rules (INT output):
    #    NULL deceased  -> DEFAULT_ERROR_INTEGER
    #    DA or deceased -> 3
    #    else           -> 0
    indicator_expr = (
        F.when(deceased_is_null, F.lit(constants.DEFAULT_ERROR_INTEGER))
         .when(status_is_DA | deceased_is_true,
               F.lit(constants.SbfeAccountUpdateDeleteIndicator.THREE.value))
         .otherwise(F.lit(constants.SbfeAccountUpdateDeleteIndicator.ZERO.value))
    )

    return joined.select(
        F.col(ABSegment.account_id.str),
        indicator_expr.alias(ABSegment.ab_update_ind.str)
    )





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

def _make_status_stub(spark, cases):
    rows = [{BaseSegment.account_id: a, BaseSegment.account_status: s} for a, s in list(cases)]
    df = create_partially_filled_dataset(spark, BaseSegment, data=rows)
    return df.select(
        F.col(BaseSegment.account_id.str).alias(ABSegment.account_id.str),
        F.col(BaseSegment.account_status.str),
    )

def _make_customer_stub(spark, cases):
    rows = [{
        CustomerInformation.account_id: a,
        CustomerInformation.is_account_holder_deceased: d
    } for a, d in list(cases)]
    df = create_partially_filled_dataset(spark, CustomerInformation, data=rows)
    return df.select(
        F.col(CustomerInformation.account_id.str).alias(ABSegment.account_id.str),
        F.col(CustomerInformation.is_account_holder_deceased.str),
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

    status_stub = _make_status_stub(spark, [(a, s) for a, s, _ in cases])
    mock_calc_status.return_value = status_stub
    cust_stub = _make_customer_stub(spark, [(a, d) for a, _, d in cases])

    empty = create_partially_filled_dataset(spark, BaseSegment, data=[])

    uut_df = calculate_acc_update_delete_ind(
        customer_information_df=cust_stub,
        account_df=empty,
        customer_df=empty,
        recoveries_df=empty,
        fraud_df=empty,
        generated_fields_df=empty,
        caps_df=empty,
    )

    # assert INT indicator only
    result_df = uut_df.select(
        F.col(ABSegment.ab_update_ind.str).cast("int").alias(ABSegment.ab_update_ind.str)
    )

    def expected_ind(status, deceased):
        if deceased is None:
            return int(constants.DEFAULT_ERROR_INTEGER)
        if (status or "").strip().lower() == constants.AccountStatus.DA.value or deceased is True:
            return int(constants.SbfeAccountUpdateDeleteIndicator.THREE.value)
        return int(constants.SbfeAccountUpdateDeleteIndicator.ZERO.value)

    expected_rows = [{ABSegment.ab_update_ind: expected_ind(s, d)} for _, s, d in cases]
    expected_df = create_partially_filled_dataset(spark, ABSegment, data=expected_rows).select(
        F.col(ABSegment.ab_update_ind.str).cast("int").alias(ABSegment.ab_update_ind.str)
    )

    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )





