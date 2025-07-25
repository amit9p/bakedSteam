
expected_rows = [
    ( "A11",    11 ),
    ( "A13_AU", 16 ),
    ( "A13_X",  30 ),
    ( "A64_AU", 16 ),
    ( "A64_X",   9 ),
    ( "A97_AH",  2 ),
    ( "A97_X",  11 ),   # ← now correct
    ( "A71",    11 ),
    ( "A78",    11 ),
    ( "A80",    11 ),
    ( "A82",    11 ),
    ( "A83",    11 ),
    ( "ADA",     5 ),
    ( "A84",  c.DEFAULT_ERROR_INTEGER ),
]

expected_df = (
    spark.createDataFrame(expected_rows, ["account_id","account_status_1"])
    # or use create_partially_filled_dataset with ABSegment
)


assert_df_equality(
    result_df,
    expected_df,
    ignore_row_order=True,
    ignore_column_order=True,
    ignore_nullable=True,    # ← handles the nullable mismatch
)

______
# tests/test_account_status_1.py
from chispa import assert_df_equality
from unittest.mock import patch

from ecbr_card_self_service.schemas.base_segment import BaseSegment
from typedspark import create_partially_filled_dataset
import ecbr_card_self_service.ecbr_calculations.constants as c

# ────────────────────────────────────────────────────────────────
# helpers – build two-column DataFrames the same way as example
# ────────────────────────────────────────────────────────────────
def make_stub_acc_status_df(spark, cases):
    """
    cases -> iterable of (account_id, account_status)
    returns DF(account_id, account_status)
    """
    rows = [
        {
            BaseSegment.account_id:     acct,
            BaseSegment.account_status: status,
        }
        for acct, status in cases
    ]
    return (
        create_partially_filled_dataset(spark, BaseSegment, data=rows)
        .select(BaseSegment.account_id, BaseSegment.account_status)
    )


def make_stub_scc_df(spark, cases):
    """
    cases -> iterable of (account_id, special_comment_code | None)
    returns DF(account_id, special_comment_code)
    """
    rows = [
        {
            BaseSegment.account_id:           acct,
            BaseSegment.special_comment_code: scc,
        }
        for acct, scc in cases
    ]
    return (
        create_partially_filled_dataset(spark, BaseSegment, data=rows)
        .select(BaseSegment.account_id, BaseSegment.special_comment_code)
    )


# ────────────────────────────────────────────────────────────────
# main test
# ────────────────────────────────────────────────────────────────
@patch(
    "<<<change-this-path>>>.calculate_account_status"          # helper #1
)
@patch(
    "<<<change-this-path>>>.calculate_special_comment_code"    # helper #2
)
def test_account_status_1_rules(
    mock_calc_scc,
    mock_calc_acc,
    spark,
):
    # 1️⃣  input cases hitting **all** outputs (2,5,9,11,16,30 + error)
    acc_cases = [
        ("A11", "11"),
        ("A13_AU", "13"),
        ("A13_X",  "13"),
        ("A64_AU", "64"),
        ("A64_X",  "64"),
        ("A97_AH", "97"),
        ("A97_X",  "97"),
        ("A71", "71"), ("A78", "78"), ("A80", "80"), ("A82", "82"), ("A83", "83"),
        ("ADA", "DA"),
        ("A84", "84"),                    # hits ELSE branch
    ]
    scc_cases = [
        ("A13_AU", "AU"),
        ("A13_X",  "AT"),
        ("A64_AU", "AU"),
        ("A64_X",  "AT"),
        ("A97_AH", "AH"),
        ("A97_X",  "AT"),
        # others default to NULL
    ]

    # 2️⃣  build stub DFs & wire into the patched helpers
    stub_acc_df = make_stub_acc_status_df(spark, acc_cases)
    stub_scc_df = make_stub_scc_df(spark, scc_cases)
    mock_calc_acc.return_value = stub_acc_df
    mock_calc_scc.return_value = stub_scc_df

    empty_df = create_partially_filled_dataset(
        spark,
        BaseSegment,
        data={BaseSegment.account_id: [], BaseSegment.account_status: []},
    )

    # 3️⃣  call the *real* rule function
    from <<<change-this-path>>> import calculate_account_status_1      # noqa: E402

    result_df = (
        calculate_account_status_1(
            account_df=stub_acc_df,
            customer_df=empty_df,
            recoveries_df=empty_df,
            fraud_df=empty_df,
            generated_fields_df=stub_scc_df,
            caps_df=empty_df,
        )
        .select(BaseSegment.account_id, BaseSegment.account_status_1)
    )

    # 4️⃣  expected mapping
    status_lookup = {
        "11": 11,
        "13_AU": 16,
        "13_X":  30,
        "64_AU": 16,
        "64_X":   9,
        "97_AH":  2,
        "97_X":  11,
        "71": 11, "78": 11, "80": 11, "82": 11, "83": 11,
        "DA": 5,
        "84": c.DEFAULT_ERROR_INTEGER,          # else/error constant
    }

    expected_rows = [
        {
            BaseSegment.account_id:     acct,
            BaseSegment.account_status_1: status_lookup.get(
                f"{status}_AU" if acct in ("A13_AU", "A64_AU") else
                f"{status}_X"  if acct in ("A13_X",  "A64_X") else
                f"{status}_AH" if acct == "A97_AH" else
                status
            ),
        }
        for acct, status in acc_cases
    ]
    expected_df = (
        create_partially_filled_dataset(spark, BaseSegment, data=expected_rows)
        .select(BaseSegment.account_id, BaseSegment.account_status_1)
    )

    # 5️⃣  compare
    assert_df_equality(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_column_order=True,
    )
