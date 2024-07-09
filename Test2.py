
import pytest
import logging
from your_module import read_parquet_based_on_date_and_runid

def test_read_parquet_based_on_date_and_runid(caplog):
    # Define test inputs
    input_df = {"data": [1, 2, 3]}
    business_date = "2024-07-09"
    run_id = "12345"
    file_type = "parquet"

    # Run the function and capture logs
    with caplog.at_level(logging.INFO):
        result = read_parquet_based_on_date_and_runid(input_df, business_date, run_id, file_type)

    # Check info logs
    assert "read based on business date and run id" in caplog.text
    assert business_date in caplog.text
    assert run_id in caplog.text
    assert file_type in caplog.text
    
    # Assert the result
    assert result == {"key1": input_df}

    # Simulate an exception and assert the logger catches it
    with caplog.at_level(logging.ERROR):
        try:
            raise Exception("Test exception")
        except Exception as e:
            read_parquet_based_on_date_and_runid(input_df, business_date, run_id, file_type)
            assert "Exception caught with Test exception" in caplog.text
