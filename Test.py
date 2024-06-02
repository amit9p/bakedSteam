

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from assembler import YourClass  # Adjust this import to match your actual module/class structure

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local").appName("TestApp").getOrCreate()

def test_read_whole_parquet_file(spark_session):
    # Assuming the path to the Parquet file relative to the root of your project
    parquet_path = "tests/resources/input/TKNZD_SAMPLE.parquet"  # Update this path if necessary

    # Initialize the class with the Spark session
    instance = YourClass(spark_session)

    # Mock the read.parquet method to prevent actual file I/O in unit tests
    with patch.object(spark_session.read, 'parquet', return_value=MagicMock()) as mock_read_parquet:
        df = instance.read_whole_parquet_file(parquet_path)

        # Assertions
        mock_read_parquet.assert_called_once_with(parquet_path)
        assert df is not None, "DataFrame should not be None"
