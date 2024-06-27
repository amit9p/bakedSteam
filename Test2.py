
import pytest
from unittest.mock import Mock, patch
import parquet_reader

def test_read_parquet_file_success(spark):
    path = "tests/resources/input/TKNZD_SAMPLE.parquet"
    df = parquet_reader.read_parquet_file(spark, path)
    
    # Assert that the DataFrame has exactly 1003 rows
    expected_row_count = 1003
    actual_row_count = df.count()
    assert actual_row_count == expected_row_count, f"Expected {expected_row_count} rows, but got {actual_row_count}"

if __name__ == "__main__":
    pytest.main()
