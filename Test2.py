
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, trim, lit, col

from ecbr_card_self_service.ecbr_calculations.utils.null_utility import check_if_any_are_null
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.ecbr_calculations.constants import DEFAULT_ERROR_DATE

def date_closed(df: DataFrame) -> DataFrame:
    """
    :param df: DataFrame: Input DataFrame
    :return: DataFrame: Output DataFrame with account_id and date_closed
    """
    result_df = df.withColumn(
        BaseSegment.date_closed.str,
        when(
            check_if_any_are_null(col(CCAccount.account_close_date.str), col(CCAccount.charge_off_date.str)),
            lit(DEFAULT_ERROR_DATE)
        )
        .when(
            trim(col(CCAccount.account_close_date.str)) == "",
            lit(None)
        )
        .when(
            col(CCAccount.account_close_date.str).isNotNull(),
            col(CCAccount.account_close_date.str)
        )
        .otherwise(col(CCAccount.charge_off_date.str))
    )

    return result_df.select(BaseSegment.account_id, BaseSegment.date_closed)
