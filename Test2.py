
import pytest
from token_util import replace_tokenized_values

def test_replace_tokenized_values_exception(capfd, caplog):
    # Arrange
    df_input = None  # Replace with actual DataFrame if needed
    token_cache_df = None  # Replace with actual DataFrame if needed

    # Temporarily modify the function to raise an exception
    def raise_exception(*args, **kwargs):
        logger.info("replacing tokenized values")
        raise Exception("Test Exception")

    # Act
    original_function = replace_tokenized_values
    try:
        replace_tokenized_values = raise_exception
        with caplog.at_level("INFO"):
            result = replace_tokenized_values(df_input, token_cache_df)
    except Exception as e:
        result = None
        print("Error occurred:", e)
    finally:
        replace_tokenized_values = original_function

    # Assert
    out, err = capfd.readouterr()
    assert result is None
    assert "replacing tokenized values" in caplog.text
    assert "Error occurred:" in out
    assert "Test Exception" in out

if __name__ == "__main__":
    pytest.main()
