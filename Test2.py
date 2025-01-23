

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def calculate_consumer_information_indicator(input_df: DataFrame) -> DataFrame:
    """
    Calculate Consumer Information Indicator based on Bankruptcy Chapter and Status.

    :param input_df: Input DataFrame containing columns 'Bankruptcy Chapter' and 'Bankruptcy Status'
    :return: DataFrame with 'Account ID' and 'Consumer Information Indicator'
    """
    try:
        result_df = input_df.withColumn(
            "Consumer Information Indicator",
            when(
                (col("Bankruptcy Status") == "Open") & (col("Bankruptcy Chapter") == "07"), "A"
            ).when(
                (col("Bankruptcy Status") == "Open") & (col("Bankruptcy Chapter") == "11"), "B"
            ).when(
                (col("Bankruptcy Status") == "Open") & (col("Bankruptcy Chapter") == "12"), "C"
            ).when(
                (col("Bankruptcy Status") == "Open") & (col("Bankruptcy Chapter") == "13"), "D"
            ).when(
                (col("Bankruptcy Status") == "Discharge") & (col("Bankruptcy Chapter") == "07"), "E"
            ).when(
                (col("Bankruptcy Status") == "Discharge") & (col("Bankruptcy Chapter") == "11"), "F"
            ).when(
                (col("Bankruptcy Status") == "Discharge") & (col("Bankruptcy Chapter") == "12"), "G"
            ).when(
                (col("Bankruptcy Status") == "Discharge") & (col("Bankruptcy Chapter") == "13"), "H"
            ).when(
                (col("Bankruptcy Status").isin("Closed", "Dismissed")), "Q"
            ).otherwise(None)
        )
        return result_df.select("Account ID", "Consumer Information Indicator")
    except Exception as e:
        raise ValueError(f"Error processing data: {e}")


import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("pytest").getOrCreate()

def test_calculate_consumer_information_indicator_valid(spark):
    from your_module import calculate_consumer_information_indicator

    schema = StructType([
        StructField("Account ID", StringType(), True),
        StructField("Bankruptcy Chapter", StringType(), True),
        StructField("Bankruptcy Status", StringType(), True)
    ])

    input_data = [
        ("1", "07", "Open"),
        ("2", "11", "Open"),
        ("3", "12", "Discharge"),
        ("4", "13", "Closed"),
        ("5", "07", "Dismissed"),
        ("6", None, "Open"),
        ("7", "13", None),
    ]

    input_df = spark.createDataFrame(input_data, schema)

    result_df = calculate_consumer_information_indicator(input_df)

    expected_data = [
        ("1", "A"),
        ("2", "B"),
        ("3", "G"),
        ("4", "Q"),
        ("5", "Q"),
        ("6", None),
        ("7", None),
    ]

    expected_df = spark.createDataFrame(expected_data, ["Account ID", "Consumer Information Indicator"])

    assert result_df.collect() == expected_df.collect()

def test_calculate_consumer_information_indicator_invalid_column(spark):
    from your_module import calculate_consumer_information_indicator

    schema = StructType([
        StructField("Account ID", StringType(), True),
        StructField("Invalid Column", StringType(), True),
        StructField("Bankruptcy Status", StringType(), True)
    ])

    input_data = [
        ("1", "07", "Open"),
    ]

    input_df = spark.createDataFrame(input_data, schema)

    with pytest.raises(ValueError, match="Error processing data:"):
        calculate_consumer_information_indicator(input_df)

def test_calculate_consumer_information_indicator_invalid_values(spark):
    from your_module import calculate_consumer_information_indicator

    schema = StructType([
        StructField("Account ID", StringType(), True),
        StructField("Bankruptcy Chapter", StringType(), True),
        StructField("Bankruptcy Status", StringType(), True)
    ])

    input_data = [
        ("1", "99", "Open"),  # Invalid Bankruptcy Chapter
        ("2", "11", "Unknown"),  # Invalid Bankruptcy Status
    ]

    input_df = spark.createDataFrame(input_data, schema)

    result_df = calculate_consumer_information_indicator(input_df)

    expected_data = [
        ("1", None),  # No match for invalid chapter
        ("2", None),  # No match for invalid status
    ]

    expected_df = spark.createDataFrame(expected_data, ["Account ID", "Consumer Information Indicator"])

    assert result_df.collect() == expected_df.collect()

def test_calculate_consumer_information_indicator_missing_column(spark):
    from your_module import calculate_consumer_information_indicator

    schema = StructType([
        StructField("Account ID", StringType(), True),
        StructField("Bankruptcy Status", StringType(), True)
        # Missing "Bankruptcy Chapter"
    ])

    input_data = [
        ("1", "Open"),
    ]

    input_df = spark.createDataFrame(input_data, schema)

    with pytest.raises(ValueError, match="Error processing data:"):
        calculate_consumer_information_indicator(input_df)
