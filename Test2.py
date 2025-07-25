
# test_account_status_1.py
import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ── adjust these three lines to your actual package structure ─────────────
import your_project.fields.account_status_1 as mod             # <- module with calculate_account_status_1
from your_project.test_utils import create_partially_filled_dataset
from your_project.constants import DEFAULT_ERROR_INTEGER
# ──────────────────────────────────────────────────────────────────────────

# ───────────────────────── spark fixture ─────────────────────────────────
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-account-status-1")
        .getOrCreate()
    )

# ──────────────────────────── the test ───────────────────────────────────
def test_account_status_1_all_outputs(spark, monkeypatch):
    # ── 1. source rows ────────────────────────────────────────────────────
    account_rows = [
        #  Field 17A only (no SCC required)
        {"account_id": "A11", "account_status": "11"},   # → 11
        {"account_id": "A71", "account_status": "71"},   # → 11 (rule-8 set)
        {"account_id": "A78", "account_status": "78"},   # → 11
        {"account_id": "A80", "account_status": "80"},   # → 11
        {"account_id": "A82", "account_status": "82"},   # → 11
        {"account_id": "A83", "account_status": "83"},   # → 11
        {"account_id": "ADA", "account_status": "DA"},   # → 5
        {"account_id": "A84", "account_status": "84"},   # → else / error
        # combos that need SCC
        {"account_id": "A13_AU", "account_status": "13"},
        {"account_id": "A13_X",  "account_status": "13"},
        {"account_id": "A64_AU", "account_status": "64"},
        {"account_id": "A64_X",  "account_status": "64"},
        {"account_id": "A97_AH", "account_status": "97"},
        {"account_id": "A97_X",  "account_status": "97"},
    ]

    special_rows = [
        {"account_id": "A13_AU", "special_comment_code": "AU"},
        {"account_id": "A13_X",  "special_comment_code": "AT"},  # anything ≠ AU
        {"account_id": "A64_AU", "special_comment_code": "AU"},
        {"account_id": "A64_X",  "special_comment_code": "AT"},
        {"account_id": "A97_AH", "special_comment_code": "AH"},
        {"account_id": "A97_X",  "special_comment_code": "AT"},  # anything ≠ AH
        # others default to NULL
    ]

    # ── 2. build DataFrames with helper ───────────────────────────────────
    acct_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("account_status", StringType(), False),
    ])
    scc_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("special_comment_code", StringType(), True),
    ])

    test_acct_df = create_partially_filled_dataset(spark, account_rows, schema=acct_schema)
    test_scc_df  = create_partially_filled_dataset(spark, special_rows,  schema=scc_schema)
    empty_df     = create_partially_filled_dataset(spark, [], schema=acct_schema)

    # ── 3. monkey-patch upstream helpers used in account_status_1 ─────────
    def stub_calculate_account_status(*_args, **_kwargs):
        # must return cols: account_id, account_status
        return test_acct_df

    def stub_calculate_special_comment_code(*_args, **_kwargs):
        # must return cols: account_id, special_comment_code
        return test_scc_df

    monkeypatch.setattr(mod, "calculate_account_status", stub_calculate_account_status)
    monkeypatch.setattr(mod, "calculate_special_comment_code", stub_calculate_special_comment_code)

    # ── 4. run the real function ──────────────────────────────────────────
    result_df = (
        mod.calculate_account_status_1(
            account_df=test_acct_df,
            customer_df=empty_df,
            recoveries_df=empty_df,
            fraud_df=empty_df,
            generated_fields_df=test_scc_df,
            caps_df=empty_df,
        )
        .select("account_id", "account_status_1")  # keep only what we assert on
    )

    # ── 5. expected DataFrame ────────────────────────────────────────────
    expected_rows = [
        {"account_id": "A11",    "account_status_1": 11},
        {"account_id": "A71",    "account_status_1": 11},
        {"account_id": "A78",    "account_status_1": 11},
        {"account_id": "A80",    "account_status_1": 11},
        {"account_id": "A82",    "account_status_1": 11},
        {"account_id": "A83",    "account_status_1": 11},
        {"account_id": "A13_AU", "account_status_1": 16},
        {"account_id": "A64_AU", "account_status_1": 16},
        {"account_id": "A13_X",  "account_status_1": 30},
        {"account_id": "A64_X",  "account_status_1": 9},
        {"account_id": "A97_AH", "account_status_1": 2},
        {"account_id": "A97_X",  "account_status_1": 11},
        {"account_id": "ADA",    "account_status_1": 5},
        {"account_id": "A84",    "account_status_1": DEFAULT_ERROR_INTEGER},
    ]
    expected_schema = StructType([
        StructField("account_id", StringType(), False),
        StructField("account_status_1", IntegerType(), True),
    ])
    expected_df = create_partially_filled_dataset(spark, expected_rows, schema=expected_schema)

    # ── 6. assert equality (row / col order agnostic) ─────────────────────
    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
