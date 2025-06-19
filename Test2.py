
from datetime import datetime
from ecbr_card_self_service.ecbr_calculations.fields.date_closed import date_closed
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.ecbr_calculations.constants import DEFAULT_ERROR_DATE
from tests.ecbr_calculations.unit_tests.base.utils import create_partially_filled_dataset


def test_date_closed(spark):
    # Input data
    data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {
                CCAccount.account_id: "1",
                CCAccount.account_close_date: datetime(2024, 12, 9),
                CCAccount.charge_off_date: datetime(2024, 12, 10),
            },
            {
                CCAccount.account_id: "2",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: datetime(2024, 12, 8),
            },
            {
                CCAccount.account_id: "3",
                CCAccount.account_close_date: "",
                CCAccount.charge_off_date: datetime(2024, 12, 7),
            },
            {
                CCAccount.account_id: "4",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: None,
            },
        ]
    )

    # Expected output
    expected = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {
                BaseSegment.account_id: "1",
                BaseSegment.date_closed: datetime(2024, 12, 9),
            },
            {
                BaseSegment.account_id: "2",
                BaseSegment.date_closed: datetime(2024, 12, 8),
            },
            {
                BaseSegment.account_id: "3",
                BaseSegment.date_closed: datetime(2024, 12, 7),
            },
            {
                BaseSegment.account_id: "4",
                BaseSegment.date_closed: DEFAULT_ERROR_DATE,
            },
        ]
    )

    # Run transformation
    result_df = date_closed(data)

    # Assert
    assert_df_equality(result_df, expected, ignore_row_order=True)

____
from datetime import datetime
from edq.ecbr_card_self_service.ecbr_calculations.fields.base.date_closed import date_closed
from edq.ecbr_card_self_service.constants import DEFAULT_ERROR_DATE, DATE_FORMAT
from edq.ecbr_card_self_service.tests.helpers.test_utils import create_partially_filled_dataset
from edq.ecbr_card_self_service.schemas.base_segment import BaseSegment
from edq.ecbr_card_self_service.schemas.cc_account import CCAccount
from chispa.dataframe_comparer import assert_df_equality


def test_date_closed(spark):
    # ✅ Input data using utility
    data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {
                CCAccount.account_id: "1",
                CCAccount.account_close_date: datetime(2024, 12, 9).date(),
                CCAccount.charge_off_date: datetime.strptime("2024-12-10", DATE_FORMAT).date(),
            },
            {
                CCAccount.account_id: "2",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: datetime.strptime("2024-12-08", DATE_FORMAT).date(),
            },
            {
                CCAccount.account_id: "3",
                CCAccount.account_close_date: "",
                CCAccount.charge_off_date: datetime.strptime("2024-12-07", DATE_FORMAT).date(),
            },
            {
                CCAccount.account_id: "4",
                CCAccount.account_close_date: None,
                CCAccount.charge_off_date: datetime.strptime("2024-12-06", DATE_FORMAT).date(),
            },
        ],
    )

    # ✅ Expected result
    expected = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {
                BaseSegment.account_id: "1",
                BaseSegment.date_closed: datetime(2024, 12, 9).date(),
            },
            {
                BaseSegment.account_id: "2",
                BaseSegment.date_closed: DEFAULT_ERROR_DATE,
            },
            {
                BaseSegment.account_id: "3",
                BaseSegment.date_closed: None,
            },
            {
                BaseSegment.account_id: "4",
                BaseSegment.date_closed: DEFAULT_ERROR_DATE,
            },
        ],
    )

    # ✅ Run transformation
    result_df = date_closed(data)

    # ✅ Assert
    assert_df_equality(result_df, expected, ignore_row_order=True)


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
