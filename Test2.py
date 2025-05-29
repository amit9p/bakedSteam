
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import DateType
from ecbr_card_self_service.schemas.sbfe.cc_account import CCAccount
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment

def date_account_was_originally_opened(ccaccount_df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with account_id and date_account_was_originally_opened.
    Uses column names from CCAccount and ABSegment classes to avoid hardcoding.
    If account_open_date is missing/invalid, defaults to None.
    """
    return ccaccount_df.withColumn(
        ABSegment.date_account_was_originally_opened.str,
        when(
            col(CCAccount.account_open_date.str).isNotNull(),
            col(CCAccount.account_open_date.str)
        ).otherwise(lit(None).cast(DateType()))
    ).select(
        CCAccount.account_id,
        ABSegment.date_account_was_originally_opened
    )



import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date

# Import your schema classes and method
from ecbr_card_self_service.schemas.sbfe.cc_account import CCAccount
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from your_module import date_account_was_originally_opened  # Update 'your_module' accordingly

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

def test_date_account_was_originally_opened_valid_and_null(spark):
    # Sample input data
    data = [
        {CCAccount.account_id.str: "A1", CCAccount.account_open_date.str: date(2021, 1, 10)},
        {CCAccount.account_id.str: "A2", CCAccount.account_open_date.str: None}
    ]
    schema = StructType([
        StructField(CCAccount.account_id.str, StringType(), True),
        StructField(CCAccount.account_open_date.str, DateType(), True)
    ])
    ccaccount_df = spark.createDataFrame([Row(**row) for row in data], schema=schema)

    # Call the function
    result_df = date_account_was_originally_opened(ccaccount_df)
    result = {row[ABSegment.account_id.str]: row[ABSegment.date_account_was_originally_opened.str] for row in result_df.collect()}

    # Check results
    assert result["A1"] == date(2021, 1, 10)
    assert result["A2"] is None


def test_all_null_dates(spark):
    data = [
        {CCAccount.account_id.str: "A1", CCAccount.account_open_date.str: None},
        {CCAccount.account_id.str: "A2", CCAccount.account_open_date.str: None},
    ]
    schema = StructType([
        StructField(CCAccount.account_id.str, StringType(), True),
        StructField(CCAccount.account_open_date.str, DateType(), True)
    ])
    ccaccount_df = spark.createDataFrame([Row(**row) for row in data], schema=schema)
    result_df = date_account_was_originally_opened(ccaccount_df)
    assert all(row[ABSegment.date_account_was_originally_opened.str] is None for row in result_df.collect())

def test_empty_df(spark):
    schema = StructType([
        StructField(CCAccount.account_id.str, StringType(), True),
        StructField(CCAccount.account_open_date.str, DateType(), True)
    ])
    ccaccount_df = spark.createDataFrame([], schema=schema)
    result_df = date_account_was_originally_opened(ccaccount_df)
    assert result_df.count() == 0

def test_missing_column(spark):
    schema = StructType([
        StructField(CCAccount.account_id.str, StringType(), True)
        # Missing account_open_date
    ])
    ccaccount_df = spark.createDataFrame([Row(**{CCAccount.account_id.str: "A1"})], schema=schema)
    with pytest.raises(Exception):
        date_account_was_originally_opened(ccaccount_df)



___________________

from datetime import datetime, date
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecbr_card_self_service.schemas.sbfe.cc_account import CCAccount
from ecbr_card_self_service.ecbr_calculations.fields.ab.date_account_was_originally_opened import (
    date_account_was_originally_opened,
)
from typespark import create_partially_filled_dataset

def test_date_account_was_originally_opened(spark: SparkSession):
    # Input DataFrame simulating various edge cases
    ccaccount_data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            # Valid date
            {
                CCAccount.account_id.str: "1",
                CCAccount.account_open_date.str: date(2021, 1, 10),
            },
            # None date
            {
                CCAccount.account_id.str: "2",
                CCAccount.account_open_date.str: None,
            },
            # All valid dates
            {
                CCAccount.account_id.str: "3",
                CCAccount.account_open_date.str: date(2022, 5, 17),
            },
            # Edge case: earliest date
            {
                CCAccount.account_id.str: "4",
                CCAccount.account_open_date.str: date(1900, 1, 1),
            },
            # Edge case: all nulls
            {
                CCAccount.account_id.str: "5",
                CCAccount.account_open_date.str: None,
            },
        ],
    )

    # Expected DataFrame
    expected_data = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {
                ABSegment.account_id.str: "1",
                ABSegment.date_account_was_originally_opened.str: date(2021, 1, 10),
            },
            {
                ABSegment.account_id.str: "2",
                ABSegment.date_account_was_originally_opened.str: None,
            },
            {
                ABSegment.account_id.str: "3",
                ABSegment.date_account_was_originally_opened.str: date(2022, 5, 17),
            },
            {
                ABSegment.account_id.str: "4",
                ABSegment.date_account_was_originally_opened.str: date(1900, 1, 1),
            },
            {
                ABSegment.account_id.str: "5",
                ABSegment.date_account_was_originally_opened.str: None,
            },
        ],
    ).select(
        ABSegment.account_id,
        ABSegment.date_account_was_originally_opened,
    )

    # Call function
    result_df = date_account_was_originally_opened(ccaccount_data)

    # Assert DataFrames match
    assert_df_equality(expected_data, result_df, ignore_nullable=True)

