
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Sample implementation of the highest_credit_value function for testing
def highest_credit_value(df):
    """
    Select account_id and highest_credit_value columns from the input DataFrame.
    """
    return df.select("account_id", "highest_credit_value")


# Setting up Spark session for tests
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Pytest").getOrCreate()


@pytest.fixture
def input_dataframe(spark):
    """
    Input DataFrame for testing.
    """
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("highest_credit_value", IntegerType(), True)
    ])
    data = [("1", 1000), ("2", 5000), ("3", 0)]
    return spark.createDataFrame(data, schema)


def test_highest_credit_value_happy_path(spark, input_dataframe):
    """
    Test the happy path where values are valid and correct.
    """
    output_df = highest_credit_value(input_dataframe)
    assert "account_id" in output_df.columns
    assert "highest_credit_value" in output_df.columns
    assert output_df.count() == 3


def test_highest_credit_value_negative_credit(spark):
    """
    Test the scenario where the highest credit value is negative.
    """
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("highest_credit_value", IntegerType(), True)
    ])
    data = [("1", -100), ("2", 2000)]
    input_df = spark.createDataFrame(data, schema)
    output_df = highest_credit_value(input_df)
    
    # Assert that the output is not filtered incorrectly
    assert output_df.count() == 2


def test_highest_credit_value_missing_column(spark):
    """
    Test the scenario where a required column is missing.
    """
    schema = StructType([
        StructField("account_id", StringType(), True)
    ])
    data = [("1",), ("2",)]
    input_df = spark.createDataFrame(data, schema)
    
    with pytest.raises(Exception):
        highest_credit_value(input_df)


def test_highest_credit_value_invalid_column_type(spark):
    """
    Test the scenario where column types are invalid.
    """
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("highest_credit_value", StringType(), True)  # Should be integer
    ])
    data = [("1", "one thousand"), ("2", "five thousand")]
    input_df = spark.createDataFrame(data, schema)
    
    with pytest.raises(Exception):
        highest_credit_value(input_df)
