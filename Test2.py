
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from your_module_path import amount_charged_off_by_creditor  # adjust this import

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()

def test_amount_charged_off_by_creditor(spark):
    # Sample schema and data for input DataFrames
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("original_charge_off_amount", IntegerType(), True),
        StructField("account_status", StringType(), True),
        StructField("posted_balance", IntegerType(), True)
    ])

    data = [
        ("acc1", 1000, "97", 200),
        ("acc2", 0, "13", 0),
    ]

    # Create all 5 input DataFrames with minimal schema
    ccaccount_df = spark.createDataFrame(data, schema)
    customer_df = spark.createDataFrame([], schema)
    recoveries_df = spark.createDataFrame([], schema)
    misc_df = spark.createDataFrame([], schema)
    ecb_generated_fields_df = spark.createDataFrame([], schema)

    # Call the method
    result_df = amount_charged_off_by_creditor(
        ccaccount_df,
        customer_df,
        recoveries_df,
        misc_df,
        ecb_generated_fields_df
    )

    # Expected result
    expected_data = [
        ("acc1", "200"),  # from posted_balance
        ("acc2", "0")     # from constant zero
    ]

    expected_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("amount_charged_off_by_creditor", StringType(), True)
    ])

    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Assertion
    assert result_df.collect() == expected_df.collect()
