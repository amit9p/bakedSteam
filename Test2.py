
import pytest
from token_util import get_token_cache

def test_get_token_cache_exception(capfd, caplog):
    # Arrange
    df = None  # Replace with actual DataFrame if needed
    env = 'test_env'

    # Temporarily modify the function to raise an exception
    def raise_exception(*args, **kwargs):
        print("Inside get_token_cache function")
        raise Exception("Test Exception")

    # Act
    original_function = get_token_cache
    try:
        get_token_cache = raise_exception
        with caplog.at_level("DEBUG"):
            result = get_token_cache(df, env)
    except Exception as e:
        result = None
        print("Error occurred:", e)
    finally:
        get_token_cache = original_function

    # Assert
    out, err = capfd.readouterr()
    assert result is None
    assert "Inside get_token_cache function" in out
    assert "Token cache invoked1" not in caplog.text
    assert "Error occurred:" in out
    assert "Test Exception" in out

if __name__ == "__main__":
    pytest.main()
