

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from assembler import YourClass  # Update this with your actual import

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local").appName("TestApp").getOrCreate()

@pytest.fixture(scope="module")
def assembler_instance(spark_session):
    return YourClass(spark_session)

def test_read_whole_parquet_file(assembler_instance, spark_session):
    # Correct path to the Parquet file
    parquet_path = "tests/resources/input/TKNZD_SAMPLE.parquet"

    # Use the patch on the exact Spark session instance's read.parquet
    with patch.object(spark_session.read, 'parquet', return_value=MagicMock()) as mock_read_parquet:
        result = assembler_instance.read_whole_parquet_file(parquet_path)

        # Ensure that the mock was called correctly
        mock_read_parquet.assert_called_once_with(parquet_path)
        assert result is not None, "DataFrame should not be None"
