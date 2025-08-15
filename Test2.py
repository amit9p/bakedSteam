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
