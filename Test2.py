
import pytest
from unittest.mock import patch, MagicMock
from assembler import get_token_cache  # Replace 'assembler' with the actual module name if different

# Test Normal Execution
@patch('ecbr_logging.logging.getLogger')
def test_get_token_cache_normal(mock_getLogger):
    mock_logger = MagicMock()
    mock_getLogger.return_value = mock_logger

    df = MagicMock()
    env = 'test_env'
    result = get_token_cache(df, env)
    
    assert result is None
    mock_getLogger.assert_called_once_with('__name__')
    mock_logger.info.assert_called_once_with("Token cache invoked")

# Test Exception Handling
@patch('ecbr_logging.logging.getLogger')
def test_get_token_cache_exception(mock_getLogger):
    mock_logger = MagicMock()
    mock_getLogger.return_value = mock_logger

    with patch('assembler.get_token_cache', side_effect=Exception("Test Error")):
        df = MagicMock()
        env = 'test_env'
        
        with pytest.raises(Exception) as exc_info:
            get_token_cache(df, env)
        
        assert str(exc_info.value) == "Test Error"
        mock_getLogger.assert_called_once_with('__name__')
        mock_logger.error.assert_called_once()
