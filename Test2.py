

import pytest
from unittest.mock import patch
from credentials_utils import get_cli_creds

@pytest.fixture
def mock_load_config():
    with patch('credentials_utils.load_config') as mock:
        yield mock

@pytest.fixture
def mock_logging():
    with patch('credentials_utils.logging') as mock:
        yield mock

def test_get_cli_creds_config_not_found(mock_load_config, mock_logging):
    # Mock the load_config to return None
    mock_load_config.return_value = None
    
    # Test for ValueError
    env = 'qa'
    with pytest.raises(ValueError, match=f"Configuration could not be loaded for environment: {env}"):
        get_cli_creds("test_chamber", env)

if __name__ == "__main__":
    pytest.main([__file__])
