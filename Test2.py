

import pytest
from token_util import get_token_cache

def test_get_token_cache(caplog):
    # Arrange
    df = None  # Replace with actual DataFrame if needed
    env = 'test_env'
    
    # Act
    with caplog.at_level("DEBUG"):
        result = get_token_cache(df, env)
    
    # Assert
    assert result is None
    assert "Inside get_token_cache function" in caplog.text
    assert "Token cache invoked1" in caplog.text
    assert "Error occurred:" not in caplog.text
