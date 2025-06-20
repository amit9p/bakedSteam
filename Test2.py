


from pyspark.sql import DataFrame
from pyspark.sql.functions import when, trim, col, lit
from datetime import datetime

from ecbr_card_self_service.ecbr_calculations.utils.null_utility import check_if_any_are_null
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount

DEFAULT_ERROR_DATE = datetime.strptime("1900-01-01", "%Y-%m-%d").date()

def date_closed(df: DataFrame) -> DataFrame:
    """
    Logic:
    - If both account_close_date and charge_off_date are null -> return error value
    - If account_close_date is null or blank -> use charge_off_date
    - Else use account_close_date
    """
    result_df = df.withColumn(
        BaseSegment.date_closed.str,
        when(
            check_if_any_are_null(
                col(CCAccount.account_close_date.str), 
                col(CCAccount.charge_off_date.str)
            ),
            lit(DEFAULT_ERROR_DATE)
        ).when(
            trim(col(CCAccount.account_close_date.str)) == "",
            col(CCAccount.charge_off_date.str)
        ).otherwise(col(CCAccount.account_close_date.str))
    )

    return result_df.select(BaseSegment.account_id, BaseSegment.date_closed)
