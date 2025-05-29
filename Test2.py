

from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.appName("ParquetToCSV").getOrCreate()

# Path to your Parquet file
parquet_path = "/path/to/input.parquet"
# Path to output CSV directory
csv_output_path = "/path/to/output_csv_dir"

# Read Parquet file
df = spark.read.parquet(parquet_path)

# Write as CSV (without index, with header)
df.write.mode("overwrite").option("header", "true").csv(csv_output_path)

spark.stop()



from datetime import date
from chispa import assert_df_equality

from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecbr_card_self_service.schemas.sbfe.cc_account import CCAccount
from ecbr_card_self_service.ecbr_calculations.fields.ab.date_account_was_originally_opened import (
    date_account_was_originally_opened,
)
from typespark import create_partially_filled_dataset

def test_date_account_was_originally_opened(spark):
    # Input data: use string keys, not .str anywhere in these dicts!
    ccaccount_data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {"account_id": "1", "account_open_date": date(2021, 1, 10)},
            {"account_id": "2", "account_open_date": None},
            {"account_id": "3", "account_open_date": date(2022, 5, 17)},
            {"account_id": "4", "account_open_date": date(1900, 1, 1)},
            {"account_id": "5", "account_open_date": None},
        ],
    )

    expected_data = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {"account_id": "1", "date_account_was_originally_opened": date(2021, 1, 10)},
            {"account_id": "2", "date_account_was_originally_opened": None},
            {"account_id": "3", "date_account_was_originally_opened": date(2022, 5, 17)},
            {"account_id": "4", "date_account_was_originally_opened": date(1900, 1, 1)},
            {"account_id": "5", "date_account_was_originally_opened": None},
        ],
    ).select(
        ABSegment.account_id, ABSegment.date_account_was_originally_opened
    )

    result_df = date_account_was_originally_opened(ccaccount_data)
    assert_df_equality(expected_data, result_df, ignore_nullable=True)
