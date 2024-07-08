

import pytest
from unittest.mock import patch, MagicMock
from ecb_assembler.assemble import get_trade_lines

# Mock dependencies
@pytest.fixture
def mock_dependencies():
    with patch("ecb_assembler.assemble.get_token_cache") as mock_get_token_cache, \
         patch("ecb_assembler.assemble.read_parquet_based_on_date_and_runid") as mock_read_parquet, \
         patch("ecb_assembler.assemble.replace_tokenized_values") as mock_replace_values, \
         patch("ecb_assembler.assemble.format") as mock_format:
        mock_get_token_cache.return_value = MagicMock(name="DataFrame")
        mock_read_parquet.return_value = MagicMock(name="DataFrame")
        mock_replace_values.return_value = MagicMock(name="DataFrame")
        mock_format.return_value = "formatted_data"
        yield {
            "token_cache": mock_get_token_cache,
            "read_parquet": mock_read_parquet,
            "replace_values": mock_replace_values,
            "format": mock_format
        }

# Test case for the logic in lines 37 to 54
def test_get_trade_lines_file_type_logic(mock_dependencies):
    kwargs = {
        "env": "test_env",
        "business_dt": "2022-01-01",
        "run_id": "123",
        "input_df": "test_df",
        "file_type": "ALL"
    }
    
    result = get_trade_lines(**kwargs)
    
    # Ensure the function processes the file_type logic correctly
    assert result is not None
    assert "ALL" in result  # Check if the result contains an entry for file_type "ALL"
    
    # Validate that the read_parquet_based_on_date_and_runid was called once
    mock_dependencies["read_parquet"].assert_called_once()
    
    # Ensure the format function was called for each key in the dataframes_dict
    for key in mock_dependencies["read_parquet"].return_value.keys():
        mock_dependencies["format"].assert_any_call(mock_dependencies["read_parquet"].return_value[key], "")

# Test ValueError
def test_get_trade_lines_value_error(mock_dependencies):
    kwargs = {
        "env": "test_env",
        "business_dt": "2022-01-01",
        "run_id": "123",
        "input_df": "test_df",
        "file_type": "ALL"
    }
    
    mock_dependencies["read_parquet"].side_effect = ValueError("Test Error")
    
    with pytest.raises(ValueError):
        get_trade_lines(**kwargs)

# Test successful case
def test_get_trade_lines_success(mock_dependencies):
    kwargs = {
        "env": "test_env",
        "business_dt": "2022-01-01",
        "run_id": "123",
        "input_df": "test_df",
        "file_type": "ALL"
    }
    
    result = get_trade_lines(**kwargs)
    
    assert result is not None
    mock_dependencies["read_parquet"].assert_called_once()
