
import pytest
from token_util import get_token_cache

def test_get_token_cache(capfd):
    # Arrange
    df = None  # Replace with actual DataFrame if needed
    env = 'test_env'
    
    # Act
    result = get_token_cache(df, env)
    
    # Assert
    out, err = capfd.readouterr()
    assert result is None
    assert "Inside get_token_cache function" in out
    assert "Token cache invoked1" in out
    assert "Error occurred:" not in out

######

import pytest
from token_util import get_token_cache

def test_get_token_cache_exception(capfd):
    # Arrange
    df = None  # Replace with actual DataFrame if needed
    env = 'test_env'

    # Temporarily modify the function to raise an exception
    def raise_exception(*args, **kwargs):
        raise Exception("Test Exception")

    # Act
    original_function = get_token_cache
    try:
        get_token_cache = raise_exception
        result = get_token_cache(df, env)
    except Exception:
        result = None
    finally:
        get_token_cache = original_function

    # Assert
    out, err = capfd.readouterr()
    assert result is None
    assert "Inside get_token_cache function" in out
    assert "Token cache invoked1" not in out
    assert "Error occurred:" in out
    assert "Test Exception" in out

if __name__ == "__main__":
    pytest.main()
