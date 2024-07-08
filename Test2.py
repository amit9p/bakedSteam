
import pytest
from unittest.mock import patch, MagicMock
from ecb_assembler.assemble import get_trade_lines
from ecb_assembler.constants import TU_REC_SEP, EQ_REC_SEP, EX_REC_SEP

# Mock dependencies
@pytest.fixture
def mock_dependencies():
    with patch("ecb_assembler.assemble.get_token_cache") as mock_get_token_cache, \
         patch("ecb_assembler.assemble.read_parquet_based_on_date_and_runid") as mock_read_parquet, \
         patch("ecb_assembler.assemble.replace_tokenized_values") as mock_replace_values, \
         patch("ecb_assembler.assemble.format") as mock_format:
        mock_get_token_cache.return_value = MagicMock(name="DataFrame")
        mock_read_parquet.return_value = {
            "TU+test": MagicMock(name="DataFrame"),
            "EQ+test": MagicMock(name="DataFrame"),
            "EX+test": MagicMock(name="DataFrame"),
            "OTHER": MagicMock(name="DataFrame")
        }
        mock_replace_values.return_value = MagicMock(name="DataFrame")
        mock_format.return_value = "formatted_data"
        yield {
            "token_cache": mock_get_token_cache,
            "read_parquet": mock_read_parquet,
            "replace_values": mock_replace_values,
            "format": mock_format
        }

# Test successful case
def test_get_trade_lines_success(mock_dependencies):
    result = get_trade_lines(
        business_dt="2022-01-01",
        run_id="123",
        input_df="test_df",
        env="test_env",
        file_type="ALL"
    )
    
    assert result is not None
    assert "TU" in result
    assert "EQ" in result
    assert "EX" in result
    assert "OTHER" in result
    assert result["TU"] == "formatted_data"
    assert result["EQ"] == "formatted_data"
    assert result["EX"] == "formatted_data"
    assert result["OTHER"] == "formatted_data"
    mock_dependencies["read_parquet"].assert_called_once()

# Test the file type extraction and record_separator logic
def test_file_type_and_separator_logic(mock_dependencies):
    result = get_trade_lines(
        business_dt="2022-01-01",
        run_id="123",
        input_df="test_df",
        env="test_env",
        file_type="ALL"
    )
    
    # Verify the logic for record_separator based on file type
    assert mock_dependencies["format"].call_count == 4
    mock_dependencies["format"].assert_any_call(mock_dependencies["read_parquet"].return_value["TU+test"], TU_REC_SEP)
    mock_dependencies["format"].assert_any_call(mock_dependencies["read_parquet"].return_value["EQ+test"], EQ_REC_SEP)
    mock_dependencies["format"].assert_any_call(mock_dependencies["read_parquet"].return_value["EX+test"], EX_REC_SEP)
    mock_dependencies["format"].assert_any_call(mock_dependencies["read_parquet"].return_value["OTHER"], "")

# Test ValueError
def test_get_trade_lines_value_error(mock_dependencies):
    mock_dependencies["read_parquet"].side_effect = ValueError("Test Error")
    
    with pytest.raises(ValueError):
        get_trade_lines(
            business_dt="2022-01-01",
            run_id="123",
            input_df="test_df",
            env="test_env",
            file_type="ALL"
        )

# Additional test to ensure all keys in the dataframes_dict are processed
def test_all_keys_processed(mock_dependencies):
    result = get_trade_lines(
        business_dt="2022-01-01",
        run_id="123",
        input_df="test_df",
        env="test_env",
        file_type="ALL"
    )
    
    # Ensure all keys from read_parquet_based_on_date_and_runid are processed
    for key in mock_dependencies["read_parquet"].return_value.keys():
        file_type_extract = key.split("+")[1] if "+" in key else key
        assert file_type_extract in result
