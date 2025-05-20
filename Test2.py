
Metro2 expects fixed-width flat files

If you write "0" instead of "000000000000", it will:

Misalign the field

Shift other fields

Break the file structure

Fail validation by credit bureaus




from pyspark.sql.types import StringType
from typespark import Column, Schema

class ABSegment(Schema):
    ...
    payment_amount_scheduled: Column[StringType]  # <-- Add this line



constant_zero_payment = "000000000000"


def get_payment_amount_scheduled(ccaccount_df: DataFrame) -> DataFrame:
    """
    :param ccaccount_df: Input DataFrame with account_id
    :return: Output DataFrame with account_id and payment_amount_scheduled (hardcoded 12-digit zero string)
    """
    result_df = ccaccount_df.withColumn(
        ABSegment.payment_amount_scheduled.str,
        lit(constant_zero_payment)
    )

    return result_df.select(
        ABSegment.account_id,
        ABSegment.payment_amount_scheduled
    )

def test_get_payment_amount_scheduled(spark: SparkSession):
    ccaccount_data = create_partially_filled_dataset(
        spark,
        CCAccount,
        [
            {CCAccount.account_id: "1"},
            {CCAccount.account_id: "004004004"},
        ],
    )

    result_df = get_payment_amount_scheduled(ccaccount_data)

    expected_data = create_partially_filled_dataset(
        spark,
        ABSegment,
        [
            {
                ABSegment.account_id: "1",
                ABSegment.payment_amount_scheduled: constant_zero_payment,
            },
            {
                ABSegment.account_id: "004004004",
                ABSegment.payment_amount_scheduled: constant_zero_payment,
            },
        ],
    ).select(
        ABSegment.account_id,
        ABSegment.payment_amount_scheduled
    )

    assert_df_equality(
        result_df,
        expected_data,
        ignore_row_order=True,
        ignore_nullable=True,
    )
