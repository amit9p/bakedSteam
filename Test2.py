import logging
from pyspark.sql import DataFrame, functions as F

logger = logging.getLogger(__name__)

def business_date(df: DataFrame) -> DataFrame:
    """
    Input : CCAccount DF with columns at least [account_id, load_partition_date (DateType)]
    Output: DF [account_id, business_date] where business_date is the dataset-wide max(load_partition_date)
            applied to every row (no dedupe).
    """
    max_df = df.agg(
        F.max(F.col(CCAccount.load_partition_date.str))
         .alias(BaseSegment.business_date.str)
    )

    # Attach the single-row scalar to all rows (keeps input cardinality)
    return (
        df.select(F.col(BaseSegment.account_id.str))
          .crossJoin(max_df)                    # or: .join(F.broadcast(max_df), on=True)
    )




# tests/fields/test_business_date.py
from datetime import datetime
import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from typepark import create_partially_filled_dataset

from ecbr_card_self_service.calculations.fields.business_date import business_date
from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount


def test_business_date_happy_path(spark: SparkSession):
    # Input CCAccount rows (DateType)
    data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {
                CCAccount.account_id: "1",
                CCAccount.load_partition_date: datetime(year=2024, month=12, day=9).date(),
            },
            {
                CCAccount.account_id: "2",
                CCAccount.load_partition_date: datetime(year=2024, month=12, day=10).date(),
            },
            {
                CCAccount.account_id: "3",
                CCAccount.load_partition_date: None,
            },
            {
                CCAccount.account_id: "4",
                CCAccount.load_partition_date: None,
            },
        ],
    )

    result_df = business_date(data)

    # Expected: max(load_partition_date) = 2024-12-10 applied to ALL rows
    expected = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data=[
            {
                BaseSegment.account_id: "1",
                BaseSegment.business_date: datetime(year=2024, month=12, day=10).date(),
            },
            {
                BaseSegment.account_id: "2",
                BaseSegment.business_date: datetime(year=2024, month=12, day=10).date(),
            },
            {
                BaseSegment.account_id: "3",
                BaseSegment.business_date: datetime(year=2024, month=12, day=10).date(),
            },
            {
                BaseSegment.account_id: "4",
                BaseSegment.business_date: datetime(year=2024, month=12, day=10).date(),
            },
        ],
    ).select(BaseSegment.account_id.col, BaseSegment.business_date.col)

    assert_df_equality(result_df, expected, ignore_row_order=True)


def test_business_date_all_nulls_raises(spark: SparkSession):
    # All load_partition_date are NULL → our impl raises (adjust if you prefer a default)
    data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {CCAccount.account_id: "1", CCAccount.load_partition_date: None},
            {CCAccount.account_id: "2", CCAccount.load_partition_date: None},
        ],
    )

    with pytest.raises(ValueError):
        _ = business_date(data)
