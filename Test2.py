
# tests/ecbr_generator/test_reportable_accounts.py

import datetime as dt
import pytest
from chispa import assert_df_equality
from typespark import create_partially_filled_dataset

# ---- Schemas (update these import paths to match your repo) ----
from ecbr_tenant_card_dfs_11_self_service.schemas.ecbr_calculator_dfs_output import (  # <-- UPDATE PATH
    EcbrCalculatorOutput as Calc,
)
from ecbr_generator.schemas.reporting_override import (  # <-- UPDATE PATH
    EcbrDFSOverride as Override,
)
from ecbr_generator.schemas.ecbr_generator_dfs_accounts_primary import (  # <-- UPDATE PATH
    EcbrGeneratorDfsAccountsPrimary as Prev,  # previously-reported dataset
)

# ---- Function under test (update import path) ----
from ecbr_generator.utils.reportable_accounts import get_reportable_accounts  # <-- UPDATE PATH


def d(s: str) -> dt.date:
    """yyyy-mm-dd -> date helper for readability."""
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def ts(s: str) -> dt.datetime:
    """yyyy-mm-dd -> timestamp helper (midnight)."""
    return dt.datetime.strptime(s, "%Y-%m-%d")


@pytest.mark.usefixtures("spark")  # if you already have a spark fixture; else remove
def test_get_reportable_accounts_happy_path(spark):
    """
    Given:
      - calculator_df with 4 accounts (A1..A4)
      - reporting_override_df marking A1 as 'R', A2 as 'r', A3 as 'S' (suppressed), A4 absent
      - previously_reported_accounts_df contains A2 (should be filtered out)
    Expect:
      - output contains A1 only (and all Calc fields), since:
          * A1 has override 'R'
          * A2 had 'r' (valid) but is already reported -> filtered by left_anti
          * A3 override not 'R' -> excluded
          * A4 no override row -> excluded
    """
    # ----------------- Build inputs (only the fields we care about) -----------------
    calculator_df = create_partially_filled_dataset(
        spark,
        Calc,
        {
            Calc.account_id: ["A1", "A2", "A3", "A4"],
            # populate a couple of Calc fields to prove they pass through
            Calc.first_name: ["Ann", "Bob", "Cara", "Dan"],
            Calc.open_date: [d("2024-01-01"), d("2024-02-02"), d("2024-03-03"), d("2024-04-04")],
        },
    )

    reporting_override_df = create_partially_filled_dataset(
        spark,
        Override,
        {
            Override.account_id: ["A1", "A2", "A3"],
            Override.reporting_status: ["R", "r", "S"],  # case-insensitive 'R' is valid
            Override.initiated_date: [ts("2024-01-15"), ts("2024-02-15"), ts("2024-03-15")],
        },
    )

    previously_reported_accounts_df = create_partially_filled_dataset(
        spark,
        Prev,
        {
            Prev.account_id: ["A2"],  # A2 was previously reported -> should be removed
            # add any required key/metadata fields here if Prev schema enforces them
        },
    )

    # ----------------- Call function under test -----------------
    actual_df = get_reportable_accounts(
        calculator_df=calculator_df,
        reporting_override_df=reporting_override_df,
        previously_reported_accounts_df=previously_reported_accounts_df,
    )

    # ----------------- Build expected (all Calc fields for A1 only) -----------------
    expected_df = create_partially_filled_dataset(
        spark,
        Calc,
        {
            Calc.account_id: ["A1"],
            Calc.first_name: ["Ann"],
            Calc.open_date: [d("2024-01-01")],
        },
    )

    # We only need to compare the columns this component promises to output.
    # If your function truly returns *all* Calc fields, restricting to Calc’s columns is fine.
    calc_cols = list(Calc.__annotations__.keys())
    assert_df_equality(
        actual_df.select(*calc_cols),
        expected_df.select(*calc_cols),
        ignore_row_order=True,
        ignore_column_order=True,
        allow_precision_loss=True,
    )


@pytest.mark.usefixtures("spark")
def test_case_insensitive_r_and_no_previous_reports(spark):
    """
    If overrides are 'r' (lowercase) and there are no previously reported rows,
    all calculator rows with an 'r' override should pass through.
    """
    calculator_df = create_partially_filled_dataset(
        spark,
        Calc,
        {
            Calc.account_id: ["X1", "X2"],
            Calc.first_name: ["Xavier", "Xena"],
        },
    )

    reporting_override_df = create_partially_filled_dataset(
        spark,
        Override,
        {
            Override.account_id: ["X1", "X2"],
            Override.reporting_status: ["r", "R"],  # both should qualify
        },
    )

    previously_reported_accounts_df = create_partially_filled_dataset(
        spark,
        Prev,
        {
            Prev.account_id: [],  # none previously reported
        },
    )

    actual_df = get_reportable_accounts(
        calculator_df, reporting_override_df, previously_reported_accounts_df
    )

    expected_df = create_partially_filled_dataset(
        spark,
        Calc,
        {
            Calc.account_id: ["X1", "X2"],
            Calc.first_name: ["Xavier", "Xena"],
        },
    )

    calc_cols = list(Calc.__annotations__.keys())
    assert_df_equality(
        actual_df.select(*calc_cols),
        expected_df.select(*calc_cols),
        ignore_row_order=True,
        ignore_column_order=True,
        allow_precision_loss=True,
    )


=========
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
