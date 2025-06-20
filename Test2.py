

from datetime import datetime
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from typedspark import create_partially_filled_dataset
from ecbr_card_self_service.ecbr_calculations.fields.base.date_closed import date_closed
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount

def test_date_closed(spark: SparkSession):
    default_date_format = "%Y-%m-%d"
    
    # Input
    data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {
                CCAccount.account_id: "1",
                CCAccount.account_close_date: datetime(2024, 12, 9).date(),
                CCAccount.charge_off_date: datetime.strptime("2024-12-10", default_date_format).date(),
            },
            {
                CCAccount.account_id: "2",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: datetime.strptime("2024-12-08", default_date_format).date(),
            },
            {
                CCAccount.account_id: "3",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: datetime.strptime("2024-12-07", default_date_format).date(),
            },
            {
                CCAccount.account_id: "4",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: None,
            },
        ],
    )

    result_df = date_closed(data)

    expected_data = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {
                BaseSegment.account_id: "1",
                BaseSegment.date_closed: datetime(2024, 12, 9).date(),
            },
            {
                BaseSegment.account_id: "2",
                BaseSegment.date_closed: datetime(2024, 12, 8).date(),
            },
            {
                BaseSegment.account_id: "3",
                BaseSegment.date_closed: datetime(2024, 12, 7).date(),
            },
            {
                BaseSegment.account_id: "4",
                BaseSegment.date_closed: datetime.strptime("1900-01-01", default_date_format).date(),
            },
        ],
    )

    assert_df_equality(result_df, expected_data, ignore_row_order=True)


____


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
