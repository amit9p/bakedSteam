
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def process_social_security_numbers(df_input: DataFrame) -> DataFrame:
    """
    Processes the input DataFrame to return Account ID and encrypted SSN.

    Parameters:
        df_input (DataFrame): Input PySpark DataFrame with columns 'AccountID' and 'EncryptedSSN'.

    Returns:
        DataFrame: Output DataFrame with 'AccountID' and 'EncryptedSSN' columns.
    """
    # Select the required columns
    df_output = df_input.select("AccountID", "EncryptedSSN")
    return df_output

import pytest
from pyspark.sql import SparkSession
from your_module_name import process_social_security_numbers  # Replace with your module name

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("TestSession").getOrCreate()

def test_process_social_security_numbers_positive(spark):
    # Positive test case with valid input
    input_data = [(101, "123456789"), (102, "987654321"), (103, "111222333")]
    expected_data = [(101, "123456789"), (102, "987654321"), (103, "111222333")]

    df_input = spark.createDataFrame(input_data, ["AccountID", "SSN"])
    df_expected = spark.createDataFrame(expected_data, ["AccountID", "SSN"])

    df_result = process_social_security_numbers(df_input)
    assert df_result.collect() == df_expected.collect()

def test_process_social_security_numbers_negative(spark):
    # Negative test case with empty input
    input_data = []
    expected_data = []

    df_input = spark.createDataFrame(input_data, ["AccountID", "SSN"])
    df_expected = spark.createDataFrame(expected_data, ["AccountID", "SSN"])

    df_result = process_social_security_numbers(df_input)
    assert df_result.collect() == df_expected.collect()

def test_process_social_security_numbers_edge_case(spark):
    # Edge case with null or empty columns
    input_data = [(101, "123456789"), (102, None), (103, "")]
    expected_data = [(101, "123456789"), (102, None), (103, "")]

    df_input = spark.createDataFrame(input_data, ["AccountID", "SSN"])
    df_expected = spark.createDataFrame(expected_data, ["AccountID", "SSN"])

    df_result = process_social_security_numbers(df_input)
    assert df_result.collect() == df_expected.collect()
