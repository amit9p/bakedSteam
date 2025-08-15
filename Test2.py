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
    # derive status
    account_status_df = calculate_account_status(
        account_df, customer_information_df, recoveries_df, fraud_df, generated_fields_df, caps_df
    )

    # join to pull deceased flag alongside status
    joined = account_status_df.join(customer_information_df, on=ABSegment.account_id, how="left")

    # column names (fallbacks if constants not present)
    ACCOUNT_ID_COL = getattr(ABSegment, "account_id", "account_id")
    OUT_COL        = getattr(ABSegment, "account_update_delete_ind", "account_update_delete_ind")
    STATUS_COL     = getattr(BaseSegment, "account_status", "account_status")
    DECEASED_COL   = getattr(BaseSegment, "deceased_notification", "deceased_notification")

    # build the rule once (not a column yet)
    rule_expr = (
        F.when(F.upper(F.col(STATUS_COL)) == F.lit("DA"), F.lit(3))
         .when(F.coalesce(F.col(DECEASED_COL).cast("boolean"), F.lit(False)), F.lit(3))
         .otherwise(F.lit(0))
    )

    # format directly; no intermediate "_raw" column
    result_df = joined.select(
        F.col(ACCOUNT_ID_COL).alias(ACCOUNT_ID_COL),
        F.format_string("%03d", rule_expr).alias(OUT_COL)   # use rule_expr.cast("string") if you want '0'/'3' instead of '000'/'003'
    )

    return result_df

_______
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lower  # kept from your file (not strictly needed below)
from pyspark.sql import functions as F

import ecbr_card_self_service.calculations.constants as constants
from ecbr_card_self_service.ecbr_calculations.fields.base.account_status import (
    calculate_account_status,
)
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
    """
    Outputs:
      - account_id
      - account_update_delete_ind: '003' when Account Status == 'DA' OR deceased_notification == True, else '000'
    """

    # Derive account_status (your existing call)
    account_status_df = calculate_account_status(
        account_df,
        customer_information_df,
        recoveries_df,
        fraud_df,
        generated_fields_df,
        caps_df,
    )

    # Join to get the deceased flag alongside account_status (your existing join)
    joined = account_status_df.join(
        customer_information_df, on=ABSegment.account_id, how="left"
    )

    # Resolve column-name constants safely
    ACCOUNT_ID_COL = getattr(ABSegment, "account_id", "account_id")
    OUT_COL = getattr(ABSegment, "account_update_delete_ind", "account_update_delete_ind")
    STATUS_COL = getattr(BaseSegment, "account_status", "account_status")
    DECEASED_COL = getattr(BaseSegment, "deceased_notification", "deceased_notification")

    # Apply rules:
    # 1) If Account Status == 'DA' -> 3
    # 2) Else if deceased_notification == True -> 3
    # 3) Else -> 0
    result_df = (
        joined.withColumn(
            "account_update_delete_ind_raw",
            F.when(F.upper(F.col(STATUS_COL)) == F.lit("DA"), F.lit(3))
            .when(F.coalesce(F.col(DECEASED_COL).cast("boolean"), F.lit(False)), F.lit(3))
            .otherwise(F.lit(0)),
        )
        # Format as 3-character numeric string (right-justified via zero left-pad)
        .withColumn(OUT_COL, F.lpad(F.col("account_update_delete_ind_raw").cast("string"), 3, "0"))
        .select(F.col(ACCOUNT_ID_COL).alias(ACCOUNT_ID_COL), F.col(OUT_COL).alias(OUT_COL))
    )

    return result_df
