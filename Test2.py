
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from your_package.account_status import calculate_account_status_1  # adjust import
from chispa import assert_df_equality
from your_package.test_utils import create_partially_filled_dataset  # adjust to where this helper lives

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession
        .builder
        .master("local[2]")
        .appName("pytest-account-status-1")
        .getOrCreate()
    )

def test_all_account_status_1_rules(spark):
    # 1) Build the source rows
    account_rows = [
        {"account_id": "A11",       "account_status": "11"},
        {"account_id": "A13_AU",    "account_status": "13"},
        {"account_id": "A13_AT",    "account_status": "13"},
        {"account_id": "A13_M",     "account_status": "13"},
        {"account_id": "A64_AU",    "account_status": "64"},
        {"account_id": "A64_AT",    "account_status": "64"},
        {"account_id": "A64_M",     "account_status": "64"},
        {"account_id": "A97_AH",    "account_status": "97"},
        {"account_id": "A97_AT",    "account_status": "97"},
        {"account_id": "A71",       "account_status": "71"},
        {"account_id": "A78",       "account_status": "78"},
        {"account_id": "A80",       "account_status": "80"},
        {"account_id": "A82",       "account_status": "82"},
        {"account_id": "A83",       "account_status": "83"},
        {"account_id": "A84",       "account_status": "84"},
        {"account_id": "ADA",       "account_status": "DA"},
    ]
    special_rows = [
        {"account_id": "A13_AU", "special_comment_code": "AU"},
        {"account_id": "A13_AT", "special_comment_code": "AT"},
        {"account_id": "A13_M",  "special_comment_code": "M"},
        {"account_id": "A64_AU", "special_comment_code": "AU"},
        {"account_id": "A64_AT", "special_comment_code": "AT"},
        {"account_id": "A64_M",  "special_comment_code": "M"},
        {"account_id": "A97_AH", "special_comment_code": "AH"},
        {"account_id": "A97_AT", "special_comment_code": "AT"},
        # others → NULL
    ]

    acct_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("account_status", StringType(), False),
    ])
    special_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("special_comment_code", StringType(), True),
    ])

    account_df = create_partially_filled_dataset(
        spark, account_rows, schema=acct_schema
    )
    special_df = create_partially_filled_dataset(
        spark, special_rows, schema=special_schema
    )

    # pass empties for the unused inputs
    empty_df = create_partially_filled_dataset(spark, [], schema=acct_schema)

    # 2) invoke
    result_df = calculate_account_status_1(
        account_df=account_df,
        customer_df=empty_df,
        recoveries_df=empty_df,
        fraud_df=empty_df,
        generated_fields_df=special_df,
        caps_df=empty_df,
    )

    # 3) expected
    expected_rows = [
        {"account_id": "A11",    "account_status_1": 11},
        {"account_id": "A13_AU", "account_status_1": 16},
        {"account_id": "A13_AT", "account_status_1": 30},
        {"account_id": "A13_M",  "account_status_1": 30},
        {"account_id": "A64_AU", "account_status_1": 16},
        {"account_id": "A64_AT", "account_status_1": 9},
        {"account_id": "A64_M",  "account_status_1": 9},
        {"account_id": "A97_AH", "account_status_1": 2},
        {"account_id": "A97_AT", "account_status_1": 11},
        {"account_id": "A71",    "account_status_1": 11},
        {"account_id": "A78",    "account_status_1": 11},
        {"account_id": "A80",    "account_status_1": 11},
        {"account_id": "A82",    "account_status_1": 11},
        {"account_id": "A83",    "account_status_1": 11},
        {"account_id": "A84",    "account_status_1": None},
        {"account_id": "ADA",    "account_status_1": 5},
    ]
    expected_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("account_status_1", IntegerType(), True),
    ])
    expected_df = create_partially_filled_dataset(
        spark, expected_rows, schema=expected_schema
    )

    # 4) compare
    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
