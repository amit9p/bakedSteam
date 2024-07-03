
import pytest
from token_util import read_parquet_based_on_date_and_runid

def test_read_parquet_based_on_date_and_runid(caplog):
    # Arrange
    input_df = None  # Replace with actual DataFrame if needed
    business_date = '2024-07-03'
    run_id = 'test_run_id'
    file_type = 'parquet'
    
    # Act
    with caplog.at_level("INFO"):
        result = read_parquet_based_on_date_and_runid(input_df, business_date, run_id, file_type)
    
    # Assert
    assert result == {}
    assert "read based on business date and run id" in caplog.text
    assert "Error occurred:" not in caplog.text
