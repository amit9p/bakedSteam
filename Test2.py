
pre_co_df = account_df.select(
    BaseSegment.account_id.str,
    get_pre_co_account_status(CCAccount.past_due_status_reason).alias(PRE_CO.pre_charge_off_account_status.str)
)


joined_df = (
    account_df
    .join(account_type_df, on=BaseSegment.account_id.str, how="left")
    .join(pre_co_df, on=BaseSegment.account_id.str, how="left")  # 👈 Add this line
    .join(customer_df, ...)
    ...
)



Thanks, Tyler! Yes, you're reading it right — the helper takes a Column input and returns a Column expression that's applied at the DataFrame level (e.g., using withColumn). So it processes the full dataset at once, not individual rows. Let me know if you'd prefer we wrap it differently, but functionally it's aligned.




from pyspark.sql.column import Column
from pyspark.sql.functions import when, lit
from ecbr_card_self_service.ecbr_calculations.constants import DEFAULT_ERROR_INTEGER

# Mapping constant
PAST_DUE_STATUS_MAPPING = {
    "PastDue1To30Days": 11,
    "PastDue31To60Days": 71,
    "PastDue61To90Days": 78,
    "PastDue91To120Days": 80,
    "PastDue121To150Days": 82,
    "PastDue151To180Days": 83,
    "PastDue181To210Days": 84,
    "PastDue211To240Days": 84,
    "PastDue241To270Days": 84,
    "PastDue271To300Days": 84,
    "PastDue301To330Days": 84,
    "PastDueOver331Days": 84
}

def get_pre_co_account_status(past_due_status_reason_col: Column) -> Column:
    """
    Returns a Column expression that evaluates Pre-CO Account Status based on past_due_status_reason_col.
    """
    expr = None
    for reason, status in PAST_DUE_STATUS_MAPPING.items():
        condition = past_due_status_reason_col == reason
        expr = when(condition, lit(status)) if expr is None else expr.when(condition, lit(status))

    return expr.otherwise(lit(DEFAULT_ERROR_INTEGER))


import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from your_module import get_pre_co_account_status  # Replace with actual module
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.ecbr_calculations.constants import DEFAULT_ERROR_INTEGER

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()

def test_get_pre_co_account_status(spark):
    input_data = [
        ("A1", "PastDue1To30Days"),
        ("A2", "PastDue31To60Days"),
        ("A3", "PastDue181To210Days"),
        ("A4", "PastDueOver331Days"),
        ("A5", None),
        ("A6", "UnknownCode")
    ]

    df = spark.createDataFrame(input_data, [CCAccount.account_id.str, CCAccount.past_due_status_reason.str])

    result_df = df.withColumn(
        CCAccount.pre_charge_off_account_status.str,
        get_pre_co_account_status(col(CCAccount.past_due_status_reason.str))
    ).select(
        CCAccount.account_id.str,
        CCAccount.pre_charge_off_account_status.str
    )

    expected_data = [
        ("A1", 11),
        ("A2", 71),
        ("A3", 84),
        ("A4", 84),
        ("A5", DEFAULT_ERROR_INTEGER),
        ("A6", DEFAULT_ERROR_INTEGER)
    ]

    expected_df = spark.createDataFrame(expected_data, [CCAccount.account_id.str, CCAccount.pre_charge_off_account_status.str])

    assert result_df.collect() == expected_df.collect()
