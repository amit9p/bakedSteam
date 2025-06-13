
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from ecbr_card_self_service.schemas.ti_segment import TISegment

# Constant for Federal Tax ID/SSN Identifier as per TI Field 06 specification
FEDERAL_TAX_IDENTIFIER_CODE = "001"

def get_federal_tax_identifier(ccaccount_df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with account_id and a hardcoded federal_tax_id_ssn = '001'.
    This corresponds to the TI Field 06 definition which should always be '001'.
    """
    return ccaccount_df.select(
        ccaccount_df.account_id.alias(TISegment.account_id.str),
        lit(FEDERAL_TAX_IDENTIFIER_CODE).alias(TISegment.federal_tax_id_ssn.str)
    )




def test_get_federal_tax_identifier(spark):
    input_df = create_partially_filled_dataset(
        spark,
        CustomerInformation,
        data=[
            {"account_id": "1"},
            {"account_id": "2"}
        ]
    )

    expected_df = create_partially_filled_dataset(
        spark,
        TISegment,
        data=[
            {"account_id": "1", "federal_tax_id_ssn": "001"},
            {"account_id": "2", "federal_tax_id_ssn": "001"}
        ]
    ).select(
        TISegment.account_id,
        TISegment.federal_tax_id_ssn
    )

    result_df = passthrough.get_federal_tax_identifier(input_df)

    assert_df_equal(
        result_df,
        expected_df,
        ignore_row_order=True,
        ignore_nullable=True
    )

______________







Current Code Pattern (Repeated Conditionals)

Your logic has many repeated when(...) blocks like:

when(manual_delete_code_exists, lit(ACCOUNT_STATUS_DA))
when(is_terminal_notification_true, lit(ACCOUNT_STATUS_DA))
when(is_aged_debt_notification_true, lit(ACCOUNT_STATUS_DA))

They are clean and correct, but since many of them lead to the same output (e.g., DA), you could group related flags into one block to reduce repetition.


---

✅ Suggested Grouped Style (Optional Refactor)

when(
    manual_delete_code_exists |
    is_terminal_notification_true |
    (is_deceased_notification_true_acct_type_8a) |
    is_aged_debt_notification_true,
    lit(ACCOUNT_STATUS_DA)
)

This:

Keeps the logic tight

Reduces vertical length

Improves readability if the logic grows
