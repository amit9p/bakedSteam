
expected_data = create_partially_filled_dataset(
    spark,
    PreCo,
    data=[
        {CCAccount.account_id: "A1", PreCo.pre_charge_off_account_status: "11"},
        {CCAccount.account_id: "A2", PreCo.pre_charge_off_account_status: "71"},
        {CCAccount.account_id: "A3", PreCo.pre_charge_off_account_status: "84"},
        {CCAccount.account_id: "A4", PreCo.pre_charge_off_account_status: "84"},
        {CCAccount.account_id: "A5", PreCo.pre_charge_off_account_status: DEFAULT_ERROR_STRING},
        {CCAccount.account_id: "A6", PreCo.pre_charge_off_account_status: DEFAULT_ERROR_STRING},
    ]
)


from datetime import datetime
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from ecbr_card_self_service.ecbr_calculations.constants import DEFAULT_ERROR_STRING
from ecbr_card_self_service.ecbr_calculations.fields.base.pre_co_account_status import get_pre_co_account_status
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.schemas.pre_co_account_status import PreCo
from typedspark import create_partially_filled_dataset


def test_get_pre_co_account_status(spark: SparkSession):
    # Input
    input_data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {"account_id": "A1", "past_due_status_reason": "PastDue1To30Days"},
            {"account_id": "A2", "past_due_status_reason": "PastDue31To60Days"},
            {"account_id": "A3", "past_due_status_reason": "PastDue181To210Days"},
            {"account_id": "A4", "past_due_status_reason": "PastDueOver331Days"},
            {"account_id": "A5", "past_due_status_reason": None},
            {"account_id": "A6", "past_due_status_reason": "UnknownCode"},
        ],
    )

    # Expected
    expected_data = create_partially_filled_dataset(
        spark,
        PreCo,
        data=[
            {"account_id": "A1", "pre_charge_off_account_status": 11},
            {"account_id": "A2", "pre_charge_off_account_status": 71},
            {"account_id": "A3", "pre_charge_off_account_status": 84},
            {"account_id": "A4", "pre_charge_off_account_status": 84},
            {"account_id": "A5", "pre_charge_off_account_status": DEFAULT_ERROR_STRING},
            {"account_id": "A6", "pre_charge_off_account_status": DEFAULT_ERROR_STRING},
        ],
    )

    # Actual
    result_df = input_data.withColumn(
        PreCo.pre_charge_off_account_status.str,
        get_pre_co_account_status(col(CCAccount.past_due_status_reason.str)),
    ).select(PreCo.account_id.str, PreCo.pre_charge_off_account_status.str)

    # Assertion
    assert_df_equality(expected_data, result_df, ignore_nullable=True)




_______







from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("CleanCSV").getOrCreate()

# Input and output paths
input_path = "path/to/account_dataset.csv"
output_path = "path/to/account_dataset_cleaned.csv"

# Column to remove
column_to_remove = "pre_charge_off_account_status"

# Load CSV
df = spark.read.option("header", True).csv(input_path)

# Drop the column if it exists
if column_to_remove in df.columns:
    df = df.drop(column_to_remove)
    df.write.mode("overwrite").option("header", True).csv(output_path)
    print(f"Column '{column_to_remove}' removed and saved to {output_path}")
else:
    print(f"Column '{column_to_remove}' not found in file.")


__________





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
