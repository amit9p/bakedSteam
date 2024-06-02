
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from assembler import YourClass  # Make sure this import matches your module/class

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.master("local").appName("TestApp").getOrCreate()

def test_read_whole_parquet_file(spark_session):
    # Assuming the path to the Parquet file is relative to the root of your project
    parquet_path = "tests/resources/input/TKNZD_SAMPLE.parquet"

    with patch('pyspark.sql.DataFrameReader.parquet', return_value=MagicMock()) as mock_parquet:
        instance = YourClass(spark_session)
        result = instance.read_whole_parquet_file(parquet_path)

        mock_parquet.assert_called_once_with(parquet_path)
        assert result is not None, "DataFrame should not be None"
