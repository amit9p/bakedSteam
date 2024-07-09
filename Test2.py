
import pytest
from unittest.mock import Mock

# Import the function from your module
from your_module import read_parquet_based_on_date_and_runid

def test_read_parquet_based_on_date_and_runid(mocker):
    # Mock the logger
    mock_logger = mocker.patch('your_module.logger')
    
    # Define test inputs
    input_df = {"data": [1, 2, 3]}
    business_date = "2024-07-09"
    run_id = "12345"
    file_type = "parquet"
    
    # Call the function
    result = read_parquet_based_on_date_and_runid(input_df, business_date, run_id, file_type)
    
    # Assert logger calls
    mock_logger.info.assert_any_call("read based on business date and run id")
    mock_logger.info.assert_any_call(business_date)
    mock_logger.info.assert_any_call(run_id)
    mock_logger.info.assert_any_call(file_type)
    
    # Assert the result
    assert result == {"key1": input_df}
    
    # Simulate an exception and assert the logger catches it
    mock_logger.reset_mock()
    mock_logger.info.side_effect = Exception("Test exception")
    with pytest.raises(Exception):
        read_parquet_based_on_date_and_runid(input_df, business_date, run_id, file_type)
    mock_logger.error.assert_called_with("Exception caught with Test exception")
