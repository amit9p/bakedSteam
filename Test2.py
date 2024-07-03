
import pytest
from unittest.mock import patch, MagicMock
from utils.token_util import get_token_cache

def test_get_token_cache_returns_none():
    # Mocking DataFrame and the environment variable
    mock_df = MagicMock()
    mock_env = "test_env"

    # Use the patch to mock the logger used in your token_util.get_token_cache
    with patch('utils.token_util.logging.getLogger') as mock_getLogger:
        # Create a logger instance in your mocked getLogger
        mocked_logger = MagicMock()
        mock_getLogger.return_value = mocked_logger

        # Call the function
        result = get_token_cache(mock_df, mock_env)

        # Check that getLogger is called once with the correct name
        mock_getLogger.assert_called_once_with('utils.token_util')

        # Check that the info log is called correctly
        mocked_logger.info.assert_called_once_with("Token cache invoked")

        # Asserting the result is None as expected when no exceptions occur
        assert result is None
