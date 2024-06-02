

import pytest
from unittest.mock import MagicMock, patch
from your_module import YourClass  # Import the class containing your method

@pytest.fixture
def spark_session_mock():
    # Create a mock Spark session
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local").appName("test_app").getOrCreate()
    spark_read_mock = MagicMock()
    spark.read.parquet = spark_read_mock
    return spark, spark_read_mock

def test_read_whole_parquet_file(spark_session_mock):
    spark, spark_read_mock = spark_session_mock
    # Set up the return value for the parquet reading operation
    df_mock = MagicMock()
    spark_read_mock.return_value = df_mock

    # Create an instance of your class, passing the mocked Spark session
    your_instance = YourClass(spark)

    # Call the method under test
    result = your_instance.read_whole_parquet_file("dummy_path")

    # Assertions to check that Spark's read.parquet was called correctly
    spark.read.parquet.assert_called_once_with("dummy_path")
    assert result == df_mock, "The DataFrame returned is not as expected"

@pytest.mark.usefixtures("spark_session_mock")
def test_integration_of_read_parquet():
    # Additional tests or integration tests can go here using the same fixture
    pass
