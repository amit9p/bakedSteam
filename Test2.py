
import pytest
from unittest.mock import Mock, patch
import parquet_reader

@patch('parquet_reader.SparkSession')
def test_read_parquet_file_exception(mock_spark_session):
    # Mock the read method to raise an exception
    mock_spark = Mock()
    mock_spark.read.schema.return_value.parquet.side_effect = Exception("Test Exception")
    mock_spark_session.builder.master.return_value.appName.return_value.getOrCreate.return_value = mock_spark
    
    # Call the function with the mocked SparkSession
    result = parquet_reader.read_parquet_file(mock_spark, "invalid/path")
    
    # Check if the result is None which means the exception block was executed
    assert result is None

if __name__ == "__main__":
    pytest.main()
