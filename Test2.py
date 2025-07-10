
# 1) the ratings you actually stub out:
stub_cases = [
    ("A101", None),
    ("A102",  "002"),
    ("A103",  "003"),
    ("A104",  "004"),
    ("A105",  "005"),
    ("A106",  "006"),
    ("A107",  "007"),
    ("A108",  "XYZ"),     # this one isn’t in the 001–007 set, so it becomes C1-ERROR
]

# 2) stub it in
stub_df = make_stub_payment_df(spark, stub_cases)
mock_calc_pay.return_value = stub_df

# 3) build the expected rows _with exactly_ those values
expected_rows = [
    {
      ABSegment.account_id: acct,
      ABSegment.delinquency_status:
          C.DELQ_STATUS_NULL
            if rating is None
          else rating
            if rating in {
               C.DELQ_STATUS_001,
               C.DELQ_STATUS_002,
               C.DELQ_STATUS_003,
               C.DELQ_STATUS_004,
               C.DELQ_STATUS_005,
               C.DELQ_STATUS_006,
               C.DELQ_STATUS_007,
             }
          else C.DEFAULT_ERROR_STRING
    }
    for acct, rating in stub_cases
]
expected_df = create_partially_filled_dataset(
    spark, ABSegment, data=expected_rows
).select(ABSegment.account_id, ABSegment.delinquency_status)

‐-------




@patch(
  "ecbr_card_self_service.ecbr_calculations.small_business"
  ".small_business_charged_off.fields.ab.delinquency_status"
  ".calculate_payment_rating"
)
def test_delinquency_with_stub(mock_calc, spark):
    …


# tests/test_delinquency_status.py
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession

from chispa import assert_df_equality

# Replace these imports with your actual paths:
from ecbr_card_self_service.ebcr_calculations.small_business.small_business_charged_off.fields.ab.delinquency_status import (
    calculate_delinquency_status,
)
from ecbr_card_self_service.ebcr_calculations.small_business.small_business_charged_off.fields.ab.payment_rating import (
    calculate_payment_rating,
)
from ecbr_card_self_service.ebcr_calculations.small_business.small_business_charged_off.fields.ab.schemas.base_segment import (
    BaseSegment,
)
import ecbr_card_self_service.ebcr_calculations.constants as C

# helper to spin up a SparkSession for pytest
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("pytest") \
        .getOrCreate()

def make_stub_payment_df(spark, cases):
    """
    Build a two-column DataFrame of (account_id, payment_rating)
    cases: list of (str, Optional[str]) tuples
    """
    rows = [
        { BaseSegment.account_id: acct,
          BaseSegment.payment_rating: rating }
        for acct, rating in cases
    ]
    # create_partially_filled_dataset is your factory for small DataFrames
    from tests.utils import create_partially_filled_dataset
    return create_partially_filled_dataset(
        spark, BaseSegment, data=rows
    ).select(
        BaseSegment.account_id,
        BaseSegment.payment_rating
    )

@patch(
    # patch the name *inside* delinquency_status.py
    "ecbr_card_self_service.ebcr_calculations.small_business"
    ".small_business_charged_off.fields.ab.delinquency_status"
    ".calculate_payment_rating"
)
def test_delinquency_status_only_looks_at_payment_rating(
    mock_calc_pay,  # this is the patched calculate_payment_rating
    spark
):
    # 1) Define exactly the account→rating pairs you want to test
    stub_cases = [
        ("A101",    None),                    # should map to DELQ_STATUS_NULL
        ("A102",    "001"),                   # → DELQ_STATUS_001
        ("A103",    "005"),                   # → DELQ_STATUS_005
        ("A104",    "XYZ"),                   # → DEFAULT_ERROR_STRING
    ]

    # 2) Build & register the stub DataFrame, patch the function to return it
    stub_df = make_stub_payment_df(spark, stub_cases)
    mock_calc_pay.return_value = stub_df

    # 3) prepare “empty” inputs for everything else
    empty = make_stub_payment_df(spark, [])  # just 0 rows, correct schema

    # 4) call the code under test — it will internally call our stub
    result_df = calculate_delinquency_status(
        account_df=empty,
        customer_df=empty,
        recoveries_df=empty,
        generated_fields_df=empty,
        fraud_df=empty,
        caps_df=empty
    ).select(
        BaseSegment.account_id,
        BaseSegment.delinquency_status
    )

    # 5) build the expected mapping: account → delinquency_status
    expected_rows = []
    for acct, rating in stub_cases:
        if rating is None:
            ds = C.DELQ_STATUS_NULL
        elif rating in {C.DELQ_STATUS_001, C.DELQ_STATUS_002,
                        C.DELQ_STATUS_003, C.DELQ_STATUS_004,
                        C.DELQ_STATUS_005, C.DELQ_STATUS_006,
                        C.DELQ_STATUS_007}:
            ds = rating
        else:
            ds = C.DEFAULT_ERROR_STRING
        expected_rows.append({
            BaseSegment.account_id: acct,
            BaseSegment.delinquency_status: ds
        })

    from tests.utils import create_partially_filled_dataset
    expected_df = create_partially_filled_dataset(
        spark, BaseSegment, data=expected_rows
    ).select(
        BaseSegment.account_id,
        BaseSegment.delinquency_status
    )

    # 6) finally assert
    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
        ignore_nullable=True
    )
