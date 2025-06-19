
from datetime import datetime
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession, Row
from ecbr_card_self_service.constants import DEFAULT_ERROR_DATE
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.fields.base.date_closed import date_closed

def test_date_closed(spark: SparkSession):
    data = spark.createDataFrame([
        Row(account_id="1", account_close_date=datetime(2024, 12, 9).date(), charge_off_date=datetime(2024, 12, 10).date()),
        Row(account_id="2", account_close_date=None, charge_off_date=datetime(2024, 12, 8).date()),
        Row(account_id="3", account_close_date="", charge_off_date=datetime(2024, 12, 7).date()),
        Row(account_id="4", account_close_date=None, charge_off_date=datetime(2024, 12, 6).date())
    ])

    expected_data = spark.createDataFrame([
        Row(account_id="1", date_closed=datetime(2024, 12, 9).date()),   # valid close date
        Row(account_id="2", date_closed=DEFAULT_ERROR_DATE),             # null → error date
        Row(account_id="3", date_closed=None),                           # blank → null
        Row(account_id="4", date_closed=DEFAULT_ERROR_DATE)              # null → error date
    ])

    result_df = date_closed(data)

    assert_df_equality(result_df.select("account_id", "date_closed"), expected_data, ignore_row_order=True)


______
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
