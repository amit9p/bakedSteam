
import pytest
from unittest.mock import patch
from your_module import get_trade_lines

@patch('your_module.read_parquet_based_on_date_and_runid')
def test_get_trade_lines(mock_read_parquet):
    # Setup your arguments
    kwargs = {
        'env': 'test_env',
        'business_dt': '2022-01-01',
        'run_id': '123',
        'input_df': MagicMock(),
        'file_type': 'ALL'
    }

    # Call the function
    get_trade_lines(**kwargs)

    # Check if the mock was called
    mock_read_parquet.assert_called_once()
