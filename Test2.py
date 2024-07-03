
import pytest
from token_util import replace_tokenized_values

def test_replace_tokenized_values(caplog):
    # Arrange
    df_input = None  # Replace with actual DataFrame if needed
    token_cache_df = None  # Replace with actual DataFrame if needed
    
    # Act
    with caplog.at_level("INFO"):
        result = replace_tokenized_values(df_input, token_cache_df)
    
    # Assert
    assert result is None
    assert "replacing tokenized values" in caplog.text
    assert "Error occurred:" not in caplog.text
