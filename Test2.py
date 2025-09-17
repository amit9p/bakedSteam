
# tests/edq/output/test_date_opened.py
import datetime as dt
import pytest
from pyspark.sql import SparkSession

from chispa import assert_df_equality
from typespark import create_partially_filled_dataset

# --- import your code & schemas exactly as in your repo ---
# adjust the import paths below to match your project layout
from ecbr_calculations.fields.base.date_opened import date_opened
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.ecbr_calculator_dfs_output import CCAAccount
from ecbr_calculations.constants import constants  # for DEFAULT_ERROR_DATE

DEFAULT_DATE_FMT = "%Y-%m-%d"


# ---------- Spark fixture ----------
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("unit-tests-date-opened")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def _d(s: str) -> dt.datetime:
    """Helper to parse yyyy-mm-dd into datetime for the fixtures."""
    return dt.datetime.strptime(s, DEFAULT_DATE_FMT)


def test_date_opened_happy_path_and_error_date(spark):
    """
    Builds CCAAccount input with a mix of valid, null, and edge dates.
    Expects BaseSegment.date_opened to carry valid dates or DEFAULT_ERROR_DATE
    for null/invalid rows (the same behavior your prod func implements).
    """
    # -------- Input DF (source: CCAAccount) --------
    input_df = create_partially_filled_dataset(
        spark,
        CCAAccount,
        {
            CCAAccount.account_id:       ["1", "2", "3", "4", "5", "6", "7", "8"],
            CCAAccount.open_date: [
                _d("2024-12-05"),  # valid
                _d("2024-11-09"),  # valid
                _d("2020-03-29"),  # valid
                _d("2008-12-07"),  # valid
                _d("2019-11-02"),  # valid
                _d("2021-08-29"),  # valid
                _d("1979-12-23"),  # valid
                None,              # will become DEFAULT_ERROR_DATE
            ],
        },
    )

    # -------- Expected DF (target: BaseSegment) --------
    expected_df = create_partially_filled_dataset(
        spark,
        BaseSegment,
        {
            BaseSegment.account_id:   ["1", "2", "3", "4", "5", "6", "7", "8"],
            BaseSegment.date_opened: [
                _d("2024-12-05"),
                _d("2024-11-09"),
                _d("2020-03-29"),
                _d("2008-12-07"),
                _d("2019-11-02"),
                _d("2021-08-29"),
                _d("1979-12-23"),
                constants.DEFAULT_ERROR_DATE,  # your constant
            ],
        },
    ).select(BaseSegment.account_id, BaseSegment.date_opened)

    # -------- Run transform --------
    result_df = date_opened(input_df).select(BaseSegment.account_id, BaseSegment.date_opened)

    # -------- Assert --------
    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_column_order=True)


def test_date_opened_all_nulls_maps_to_error_date(spark):
    """If every open_date is null, all rows should use DEFAULT_ERROR_DATE."""
    input_df = create_partially_filled_dataset(
        spark,
        CCAAccount,
        {
            CCAAccount.account_id: ["a", "b", "c"],
            CCAAccount.open_date:  [None, None, None],
        },
    )

    expected_df = create_partially_filled_dataset(
        spark,
        BaseSegment,
        {
            BaseSegment.account_id:  ["a", "b", "c"],
            BaseSegment.date_opened: [
                constants.DEFAULT_ERROR_DATE,
                constants.DEFAULT_ERROR_DATE,
                constants.DEFAULT_ERROR_DATE,
            ],
        },
    ).select(BaseSegment.account_id, BaseSegment.date_opened)

    result_df = date_opened(input_df).select(BaseSegment.account_id, BaseSegment.date_opened)

    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_column_order=True)

__________________
from pyspark.sql import DataFrame, functions as F

# your schema classes
from schemas.ecbr_calculator_dfs_output import EcbrCalculatorOutput
from pre_charged_off.schemas.reporting_override import EcbrDFSOverride

# pull column names from schemas (strings)
ACC_ID  = EcbrCalculatorOutput.account_id.str
STATUS  = EcbrDFSOverride.reporting_status.str

def get_reportable_accounts(
    calculator_df: DataFrame,
    reporting_override_df: DataFrame,
    previously_reported_accounts_df: DataFrame,
) -> DataFrame:
    # 1) Overrides where reporting_status == 'R'
    overrides_R = (
        reporting_override_df
        .filter(F.upper(F.col(STATUS)) == F.lit("R"))
        .select(F.col(ACC_ID))
        .dropna(subset=[ACC_ID])
        .dropDuplicates([ACC_ID])
    )

    # 2) Eligible calculator rows (inner join to overrides)
    eligible_calc = calculator_df.join(overrides_R, on=ACC_ID, how="inner")

    # 3) Remove accounts already reported (left_anti)
    prev_ids = (
        previously_reported_accounts_df
        .select(F.col(ACC_ID))
        .dropna(subset=[ACC_ID])
        .dropDuplicates([ACC_ID])
    )
    final_df = eligible_calc.join(prev_ids, on=ACC_ID, how="left_anti")

    # 4) Return only calculator columns
    return final_df.select([F.col(c) for c in calculator_df.columns])
