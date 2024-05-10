
# test_assembler.py

import pytest
from unittest.mock import Mock
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.dataframe import DataFrame
from ecb_assmbler.assembler import Assembler

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("Test Assembler").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def mock_spark_read(spark):
    # Mock the read.parquet method
    mock_read = Mock()
    spark.read.parquet = mock_read
    return mock_read

def test_read_parquet_based_on_date_and_runid_with_mocking(spark, mock_spark_read):
    # Setup the assembler instance
    assembler = Assembler(spark)

    # Mock data
    mock_df = spark.createDataFrame([
        ("2024-03-04", "sample_run_id", "metro2-all"),
        ("2024-03-04", "sample_run_id", "metro2-equifax"),
    ], ["business_date", "run_id", "file_type"])

    # Setup the mock to return our DataFrame
    mock_spark_read.return_value = mock_df.filter(col("business_date") == "2024-03-04").filter(col("run_id") == "sample_run_id")

    # Test data
    path = "dummy_path"
    business_date = "2024-03-04"
    run_id = "sample_run_id"
    file_type = "ALL"

    # Call the method under test
    result = assembler.read_parquet_based_on_date_and_runid(path, business_date, run_id, file_type)

    # Assertions to ensure our method behaves as expected
    assert isinstance(result, dict)
    assert "metro2-all" in result and "metro2-all+equifax" in result
    assert isinstance(result["metro2-all"], DataFrame)

    # Check that the mock was called correctly
    mock_spark_read.assert_called_once_with("dummy_path")

# Additional necessary imports and setup may be required here
