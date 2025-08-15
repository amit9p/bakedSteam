
# joined already built:
# joined = account_status_df.join(customer_information_df, on=ABSegment.account_id_str, how="left")

# case-insensitive + trim to handle " da ", None, etc.
status_col   = F.trim(F.col(BaseSegment.account_status.str).cast("string"))
status_is_DA = F.upper(status_col) == F.upper(F.lit(constants.AccountStatus.DA.value))  # works for "da" or "DA"

# null-safe boolean for deceased flag
deceased_is_true = F.coalesce(
    F.col(CustomerInformation.is_account_holder_deceased.str).cast("boolean"),
    F.lit(False),
)

# business rule: DA OR deceased -> 3; else -> 0
indicator_expr = F.when(status_is_DA | deceased_is_true,
                        F.lit(constants.SbfeAccountUpdateDeleteIndicator.THREE.value)) \
                  .otherwise(F.lit(constants.SbfeAccountUpdateDeleteIndicator.ZERO.value))

result_df = joined.select(
    F.col(ABSegment.account_id_str),
    indicator_expr.alias(ABSegment.ab_update_ind_str)
)

return result_df

:::::::::::::::
# test_acc_update_delete_ind.py
from unittest.mock import patch
from chispa import assert_df_equality
from pyspark.sql import functions as F

from ecbr_card_self_service.ecbr_calculations import constants
from ecbr_card_self_service.schemas.base.segment import BaseSegment
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecbr_card_self_service.schemas.customer_information import CustomerInformation

from ecbr_card_self_service.ecbr_calculations.small_business.small_business_charged_off.fields.ab.acc_update_delete_ind import (
    calculate_acc_update_delete_ind,
)

from ecbr_card_self_service.utils.spark import create_partially_filled_dataset  # same helper used in your other test


def make_stub_account_status_df(spark, rows):
    """
    rows: list[tuple[str, str]] -> (account_id, account_status)
    Build DF with the two columns needed by the UUT join.
    """
    df = create_partially_filled_dataset(
        spark, BaseSegment, data=[{BaseSegment.account_id: a, BaseSegment.account_status: s} for a, s in rows]
    ).select(
        # ensure the join key matches ABSegment.account_id name
        F.col(BaseSegment.account_id).alias(ABSegment.account_id),
        F.col(BaseSegment.account_status).alias(BaseSegment.account_status),
    )
    return df


def make_stub_customer_info_df(spark, rows):
    """
    rows: list[tuple[str, Optional[bool]]] -> (account_id, is_account_holder_deceased)
    """
    df = create_partially_filled_dataset(
        spark,
        CustomerInformation,
        data=[{CustomerInformation.account_id: a, CustomerInformation.is_account_holder_deceased: d} for a, d in rows],
    ).select(
        F.col(CustomerInformation.account_id).alias(ABSegment.account_id),
        F.col(CustomerInformation.is_account_holder_deceased).alias(CustomerInformation.is_account_holder_deceased),
    )
    return df


@patch(
    "ecbr_card_self_service.ecbr_calculations.small_business.small_business_charged_off.fields.ab.acc_update_delete_ind.calculate_account_status"
)
def test_acc_update_delete_ind_rules(mock_calc_status, spark):
    # ---------- Arrange ----------
    # Cases: (acct, status, deceased)
    cases = [
        ("A100", constants.AccountStatus.DA.value, None),   # DA -> 3
        ("A101", "11", True),                                # deceased -> 3
        ("A102", "13", False),                               # else -> 0
        ("A103", "97", None),                                # else -> 0
        ("A104", constants.AccountStatus.DA.value, False),   # DA -> 3
    ]

    # Stub for calculate_account_status()
    stub_status_df = make_stub_account_status_df(
        spark, [(acct, status) for acct, status, _ in cases]
    )
    mock_calc_status.return_value = stub_status_df

    # Customer info with deceased flag
    customer_info_df = make_stub_customer_info_df(
        spark, [(acct, deceased) for acct, _, deceased in cases]
    )

    # Empty/skeleton inputs for unused params (schema-correct but zero rows)
    empty = create_partially_filled_dataset(spark, BaseSegment, data=[])

    # ---------- Act ----------
    result_df = calculate_acc_update_delete_ind(
        customer_information_df=customer_info_df,
        account_df=empty,
        recoveries_df=empty,
        fraud_df=empty,
        generated_fields_df=empty,
        caps_df=empty,
        abs_df=empty,
    ).select(ABSegment.account_id, ABSegment.ab_update_ind_str)

    # ---------- Expect ----------
    def expected_ind(status, deceased):
        if status == constants.AccountStatus.DA.value:
            return constants.SbfeAccountUpdateDeleteIndicator.THREE.value
        if deceased is True:
            return constants.SbfeAccountUpdateDeleteIndicator.THREE.value
        return constants.SbfeAccountUpdateDeleteIndicator.ZERO.value

    expected_rows = [
        {ABSegment.account_id: acct, ABSegment.ab_update_ind_str: expected_ind(status, deceased)}
        for acct, status, deceased in cases
    ]

    expected_df = create_partially_filled_dataset(
        spark, ABSegment, data=expected_rows
    ).select(ABSegment.account_id, ABSegment.ab_update_ind_str)

    # ---------- Assert ----------
    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True,
    )



_____________________@@@@@@
# normalize status in Spark and compare to the literal 'DA'
status_is_DA = (
    F.upper(F.col(BaseSegment.account_status.str)) == F.lit(constants.AccountStatus.DA.value)
)

# treat null as False; cast only if the source column isn't boolean
deceased_is_true = F.coalesce(
    F.col(CustomerInformation.is_account_holder_deceased.str).cast("boolean"),
    F.lit(False)
)

rule_expr = F.when(status_is_DA | deceased_is_true,
                   F.lit(constants.SbfeAccountUpdateDeleteIndicator.THREE.value)) \
             .otherwise(F.lit(constants.SbfeAccountUpdateDeleteIndicator.ZERO.value))


result_df = joined.select(
    F.col(ABSegment.account_id_str),
    rule_expr.alias(ABSegment.ab_update_ind_str)
)




from pyspark.sql import DataFrame, functions as F
from ecbr_card_self_service.ecbr_calculations.fields.base.account_status import calculate_account_status
from ecbr_card_self_service.schemas.base.segment_import import BaseSegment
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment


def calculate_acc_update_delete_ind(
    customer_information_df: DataFrame,
    account_df: DataFrame,
    recoveries_df: DataFrame,
    fraud_df: DataFrame,
    generated_fields_df: DataFrame,
    caps_df: DataFrame,
    abs_df: DataFrame,
) -> DataFrame:
    # 1) derive account status
    account_status_df = calculate_account_status(
        account_df, customer_information_df, recoveries_df, fraud_df, generated_fields_df, caps_df
    )

    # 2) join to get deceased flag alongside status
    joined = account_status_df.join(
        customer_information_df, on=ABSegment.account_id, how="left"
    )

    # 3) business rule: DA -> 3; deceased==true -> 3; else -> 0
    rule_expr = (
        F.when(F.upper(F.col(BaseSegment.account_status)) == F.lit("DA"), F.lit(3))
         .when(F.coalesce(F.col(BaseSegment.deceased_notification).cast("boolean"), F.lit(False)), F.lit(3))
         .otherwise(F.lit(0))
    )

    # 4) outputs (no 3-char padding)
    result_df = joined.select(
        F.col(ABSegment.account_id),
        rule_expr.alias(ABSegment.account_update_delete_ind)   # int 0/3
    )

    return result_df
