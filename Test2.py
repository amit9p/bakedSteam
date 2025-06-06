
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col

# Mapping as a constant (you can move this to a config/constants file)
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

def get_pre_co_account_status(ccaccount_df: DataFrame) -> DataFrame:
    """
    Assigns Pre-CO Account Status using past_due_status_reason.
    Returns a DataFrame with account_id and pre_charge_off_account_status.
    """
    expr = None
    for reason, status in PAST_DUE_STATUS_MAPPING.items():
        condition = col("past_due_status_reason") == reason
        expr = when(condition, status) if expr is None else expr.when(condition, status)

    expr = expr.otherwise("ERROR")

    return ccaccount_df.select(
        "account_id",
        expr.alias("pre_charge_off_account_status")
    )


import pytest
from pyspark.sql import SparkSession
from your_module import get_pre_co_account_status

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

def test_get_pre_co_account_status(spark):
    input_data = [
        ("A1", "PastDue1To30Days"),
        ("A2", "PastDue31To60Days"),
        ("A3", "PastDue181To210Days"),
        ("A4", "PastDueOver331Days"),
        ("A5", None),
        ("A6", "UnknownCode")
    ]

    df = spark.createDataFrame(input_data, ["account_id", "past_due_status_reason"])

    result_df = get_pre_co_account_status(df)

    expected_data = [
        ("A1", 11),
        ("A2", 71),
        ("A3", 84),
        ("A4", 84),
        ("A5", "ERROR"),
        ("A6", "ERROR")
    ]

    expected_df = spark.createDataFrame(expected_data, ["account_id", "pre_charge_off_account_status"])

    assert result_df.collect() == expected_df.collect()
