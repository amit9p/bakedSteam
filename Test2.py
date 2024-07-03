
import pytest
from unittest.mock import patch, MagicMock
from assembler import get_trade_lines  # Adjust this import according to your actual module structure

@pytest.mark.parametrize("file_type", ["ALL"])  # Add other file types as needed
def test_get_trade_lines(file_type):
    kwargs = {
        'env': 'test_env',
        'business_dt': '2022-01-01',
        'run_id': '123',
        'input_df': MagicMock(),
        'file_type': file_type
    }
    with patch('assembler.ecbr_assembler.reader.read_parquet_based_on_date_and_runid') as mock_read_parquet:
        get_trade_lines(**kwargs)
        mock_read_parquet.assert_called_once()
