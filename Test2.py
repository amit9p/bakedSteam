
# test_account_status_1.py
import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField, StringType

# ── adjust these to your repo structure ───────────────────────────────────
import your_project.fields.account_status_1 as mod
from your_project.schemas.base_segment import BaseSegment        # typed-spark schema
from your_project.test_utils import create_partially_filled_dataset
from your_project.constants import DEFAULT_ERROR_INTEGER
# ──────────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-account-status-1")
        .getOrCreate()
    )


def test_account_status_1_all_outputs(spark, monkeypatch):
    # ── 1. 𝙎𝙤𝙪𝙧𝙘𝙚 𝙙𝙖𝙩𝙖  -------------------------------------------------------------
    id_list = [
        "A11", "A71", "A78", "A80", "A82", "A83",            # rule-8 set & 11
        "ADA", "A84",                                        # 5  &  ERROR
        "A13_AU", "A13_X",                                   # 16 & 30
        "A64_AU", "A64_X",                                   # 16 &  9
        "A97_AH", "A97_X",                                   #  2 & 11
    ]
    status_list = [
        "11", "71", "78", "80", "82", "83",
        "DA", "84",
        "13", "13",
        "64", "64",
        "97", "97",
    ]

    # Special-comment codes *aligned by position* (None → NULL)
    scc_list = [
        None, None, None, None, None, None,          # A11 … A83
        None, None,                                  # ADA, A84
        "AU", "AT",                                  # A13_AU, A13_X
        "AU", "AT",                                  # A64_AU, A64_X
        "AH", "AT",                                  # A97_AH, A97_X
    ]

    # ── 2. 𝘮𝘰𝘤𝘬 Account-Status DF  ------------------------------------------------
    account_df = create_partially_filled_dataset(
        spark,
        BaseSegment,
        {
            BaseSegment.account_id:     id_list,
            BaseSegment.account_status: status_list,
        },
    )

    # ── 3. 𝘮𝘰𝘤𝘬 Special-Comment-Code DF  ------------------------------------------
    special_df = create_partially_filled_dataset(
        spark,
        BaseSegment,
        {
            BaseSegment.account_id:           id_list,
            BaseSegment.special_comment_code: scc_list,
        },
    )

    empty_df = create_partially_filled_dataset(
        spark, BaseSegment, {BaseSegment.account_id: [], BaseSegment.account_status: []}
    )

    # ── 4. patch upstream helpers so the real function uses our mocks  -----------
    monkeypatch.setattr(mod, "calculate_account_status",          lambda *_, **__: account_df)
    monkeypatch.setattr(mod, "calculate_special_comment_code",    lambda *_, **__: special_df)

    # ── 5. run the *real* calculate_account_status_1  ----------------------------
    result_df = (
        mod.calculate_account_status_1(
            account_df     = account_df,
            customer_df    = empty_df,
            recoveries_df  = empty_df,
            fraud_df       = empty_df,
            generated_fields_df = special_df,
            caps_df        = empty_df,
        )
        .select("account_id", "account_status_1")
    )

    # ── 6. expected dataframe (simple spark.createDataFrame)  --------------------
    expected_rows = [
        ("A11",    11),
        ("A71",    11),
        ("A78",    11),
        ("A80",    11),
        ("A82",    11),
        ("A83",    11),
        ("A13_AU", 16),
        ("A64_AU", 16),
        ("A13_X",  30),
        ("A64_X",   9),
        ("A97_AH",  2),
        ("A97_X",  11),
        ("ADA",     5),
        ("A84", DEFAULT_ERROR_INTEGER),
    ]
    exp_schema = StructType([
        StructField("account_id",       StringType(), False),
        StructField("account_status_1", IntegerType(), True),
    ])
    expected_df = spark.createDataFrame(expected_rows, exp_schema)

    # ── 7. assert equality  -------------------------------------------------------
    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
