

from pyspark.sql.functions import when, col, lit
from ecbr_card_self_service.ecbr_calculations.constants import DEFAULT_ERROR_INTEGER

def amount_charged_off_by_creditor(
    ccaccount_df: DataFrame,
    customer_df: DataFrame,
    recoveries_df: DataFrame,
    misc_df: DataFrame,
    ecbr_generated_fields_df: DataFrame,
) -> DataFrame:
    """
    This method calculates Field 73 by reusing the Base Field 23 logic.
    It returns a DataFrame with account_id and amount_charged_off_by_creditor.
    """
    original_charge_off_amount_df = original_charge_off_amount(
        ccaccount_df,
        customer_df,
        recoveries_df,
        misc_df,
        ecbr_generated_fields_df
    )

    return original_charge_off_amount_df.withColumn(
        ABSegment.amount_charged_off_by_creditor.str,
        when(
            col(BaseSegment.original_charge_off_amount.str).isNull(),
            lit(DEFAULT_ERROR_INTEGER)
        ).otherwise(col(BaseSegment.original_charge_off_amount.str))
    ).select(
        ABSegment.account_id.str,
        ABSegment.amount_charged_off_by_creditor.str
    )




from datetime import datetime

CustomerInformation.bankruptcy_first_filed_date: datetime(2022, 1, 1).date()
CustomerInformation.bankruptcy_first_filed_date: datetime(2022, 1, 1).date()
CustomerInformation.bankruptcy_first_filed_date: datetime(2022, 1, 2).date()
CustomerInformation.bankruptcy_first_filed_date: datetime(2022, 1, 3).date()
CustomerInformation.bankruptcy_first_filed_date: datetime(2022, 1, 18).date()



from datetime import datetime

customerInformation = create_partially_filled_dataset(
    spark,
    CustomerInformation,
    data=[
        {
            CustomerInformation.account_id: "10",
            CustomerInformation.bankruptcy_chapter: "11",
            CustomerInformation.bankruptcy_first_filed_date: datetime(2023, 1, 1).date(),
            CustomerInformation.is_account_holder_deceased: False,
            CustomerInformation.has_financial_liability: False,
        },
        {
            CustomerInformation.account_id: "20",
            CustomerInformation.bankruptcy_chapter: "11",
            CustomerInformation.bankruptcy_first_filed_date: datetime(2023, 1, 1).date(),
            CustomerInformation.is_account_holder_deceased: False,
            CustomerInformation.has_financial_liability: False,
        },
        {
            CustomerInformation.account_id: "30",
            CustomerInformation.bankruptcy_chapter: "11",
            CustomerInformation.bankruptcy_first_filed_date: datetime(2023, 1, 1).date(),
            CustomerInformation.is_account_holder_deceased: False,
            CustomerInformation.has_financial_liability: False,
        },
        {
            CustomerInformation.account_id: "40",
            CustomerInformation.bankruptcy_chapter: "11",
        }
    ]
)


CCAccount.charge_off_date: datetime(2023, 12, 31).date(),

from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import Row

schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("amount_charged_off_by_creditor", LongType(), True)
])

expected_df = spark.createDataFrame([
    Row(account_id="1", amount_charged_off_by_creditor=0),
    Row(account_id="10", amount_charged_off_by_creditor=0),
    Row(account_id="11", amount_charged_off_by_creditor=100),
    Row(account_id="12", amount_charged_off_by_creditor=0),
    Row(account_id="13", amount_charged_off_by_creditor=None),
    Row(account_id="14", amount_charged_off_by_creditor=0),
    Row(account_id="15", amount_charged_off_by_creditor=None),
    Row(account_id="16", amount_charged_off_by_creditor=0),
    Row(account_id="17", amount_charged_off_by_creditor=None),
    Row(account_id="18", amount_charged_off_by_creditor=0),
    Row(account_id="19", amount_charged_off_by_creditor=None),
    Row(account_id="2", amount_charged_off_by_creditor=100),
    Row(account_id="22", amount_charged_off_by_creditor=0),
    Row(account_id="23", amount_charged_off_by_creditor=None),
    Row(account_id="24", amount_charged_off_by_creditor=0),
    Row(account_id="25", amount_charged_off_by_creditor=0),
    Row(account_id="26", amount_charged_off_by_creditor=None),
    Row(account_id="3", amount_charged_off_by_creditor=0),
    Row(account_id="4", amount_charged_off_by_creditor=0),
    Row(account_id="5", amount_charged_off_by_creditor=0),
], schema=schema)


________

from pyspark.sql import Row

expected_df = spark.createDataFrame([
    Row(account_id="1",  amount_charged_off_by_creditor=0),
    Row(account_id="10", amount_charged_off_by_creditor=0),
    Row(account_id="11", amount_charged_off_by_creditor=100),
    Row(account_id="12", amount_charged_off_by_creditor=0),
    Row(account_id="13", amount_charged_off_by_creditor=None),
    Row(account_id="14", amount_charged_off_by_creditor=0),
    Row(account_id="15", amount_charged_off_by_creditor=None),
    Row(account_id="16", amount_charged_off_by_creditor=0),
    Row(account_id="17", amount_charged_off_by_creditor=None),
    Row(account_id="18", amount_charged_off_by_creditor=0),
    Row(account_id="19", amount_charged_off_by_creditor=None),
    Row(account_id="2",  amount_charged_off_by_creditor=100),
    Row(account_id="22", amount_charged_off_by_creditor=0),
    Row(account_id="23", amount_charged_off_by_creditor=None),
    Row(account_id="24", amount_charged_off_by_creditor=0),
    Row(account_id="25", amount_charged_off_by_creditor=0),
    Row(account_id="26", amount_charged_off_by_creditor=None),
    Row(account_id="3",  amount_charged_off_by_creditor=0),
    Row(account_id="4",  amount_charged_off_by_creditor=0),
    Row(account_id="5",  amount_charged_off_by_creditor=0),
])




import pytest
from pyspark.sql import Row
from chispa import assert_df_equality
from amount_charged_off_by_creditor import amount_charged_off_by_creditor

def test_amount_charged_off_by_creditor(spark):
    # Input DataFrames
    ccaccount_df = spark.createDataFrame([
        Row(account_id="A1", posted_balance=150),
        Row(account_id="A2", posted_balance=0),
        Row(account_id="A3", posted_balance=300)
    ])

    customer_df = spark.createDataFrame([
        Row(account_id="A1"),
        Row(account_id="A2"),
        Row(account_id="A3")
    ])

    recoveries_df = spark.createDataFrame([
        Row(account_id="A1"),
        Row(account_id="A2"),
        Row(account_id="A3")
    ])

    misc_df = spark.createDataFrame([
        Row(account_id="A1"),
        Row(account_id="A2"),
        Row(account_id="A3")
    ])

    ecbr_generated_fields_df = spark.createDataFrame([
        Row(account_id="A1", account_status="97"),
        Row(account_id="A2", account_status="11"),
        Row(account_id="A3", account_status="64")
    ])

    # Expected output
    expected_df = spark.createDataFrame([
        Row(account_id="A1", amount_charged_off_by_creditor=150),
        Row(account_id="A2", amount_charged_off_by_creditor=0),
        Row(account_id="A3", amount_charged_off_by_creditor=300)
    ])

    # Function under test
    result_df = amount_charged_off_by_creditor(
        ccaccount_df,
        customer_df,
        recoveries_df,
        misc_df,
        ecbr_generated_fields_df
    )

    # Assertion
    assert_df_equality(result_df, expected_df, ignore_nullable=True, ignore_column_order=True)
