
# test_account_status_1.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from chispa import assert_df_equality
from your_package.test_utils import create_partially_filled_dataset

# import the module under test
import your_package.account_status_1 as mod   # adjust to the actual path

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession
        .builder
        .master("local[2]")
        .appName("pytest-account-status-1")
        .getOrCreate()
    )

def test_all_account_status_1_rules(spark, monkeypatch):
    # ——————————————————————————————————————————————
    # 1) Prepare raw test rows for Field 17A & Field 19
    acct_rows = [
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
        # the others get NULL automatically
    ]

    acct_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("account_status", StringType(), False),
    ])
    spec_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("special_comment_code", StringType(), True),
    ])

    raw_acct_df    = create_partially_filled_dataset(spark, acct_rows, schema=acct_schema)
    raw_special_df = create_partially_filled_dataset(spark, special_rows, schema=spec_schema)
    empty_acct_df  = create_partially_filled_dataset(spark, [], schema=acct_schema)

    # ——————————————————————————————————————————————
    # 2) Monkeypatch the two upstream helpers so they return our raw DFs:
    def stub_account_status(
        account_df, customer_df, recoveries_df, fraud_df, generated_fields_df, caps_df
    ):
        return account_df.select("account_id", "account_status")

    def stub_special_code(account_df, recoveries_df, customer_df):
        # note: calculate_special_comment_code signature may differ; adjust args as needed
        return raw_special_df.select("account_id", "special_comment_code")

    monkeypatch.setattr(mod, "calculate_account_status", stub_account_status)
    monkeypatch.setattr(mod, "calculate_special_comment_code", stub_special_code)

    # ——————————————————————————————————————————————
    # 3) Invoke the real calculate_account_status_1
    result_df = mod.calculate_account_status_1(
        account_df=raw_acct_df,
        customer_df=empty_acct_df,
        recoveries_df=empty_acct_df,
        fraud_df=empty_acct_df,
        generated_fields_df=raw_special_df,
        caps_df=empty_acct_df,
    )

    # ——————————————————————————————————————————————
    # 4) Build expected DataFrame
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

    # ——————————————————————————————————————————————
    # 5) Assert equality
    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
