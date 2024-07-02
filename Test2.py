

import pytest
from unittest.mock import patch, MagicMock
from credentials_utils import get_cli_creds

# Sample chamber config and env config for testing
sample_chamber_config = {
    "env_config": {
        "CHAMBER_URL": "test_url",
        "VAULT_ROLE": "test_role",
        "LOCKBOX_ID": "test_lockbox_id",
        "CLIENT_ID_PATH": "test_client_id_path",
        "CLIENT_SECRET_PATH": "test_client_secret_path"
    }
}

@pytest.fixture
def mock_load_config():
    with patch('credentials_utils.load_config') as mock:
        yield mock

@pytest.fixture
def mock_logging():
    with patch('credentials_utils.logging') as mock:
        yield mock

def test_get_cli_creds_qa(mock_load_config, mock_logging):
    # Mock the load_config to return sample_chamber_config
    mock_load_config.return_value = sample_chamber_config
    
    # Test for 'qa' environment
    env = 'qa'
    creds = get_cli_creds("test_chamber", env)
    
    assert creds['client_id'] == 'CLIENTID'
    assert creds['client_secret'] == 'CLIENTSECRET'

def test_get_cli_creds_prod(mock_load_config, mock_logging):
    # Mock the load_config to return sample_chamber_config
    mock_load_config.return_value = sample_chamber_config
    
    # Mock IamClient methods
    mock_iam_instance = MagicMock()
    mock_iam_instance.get_secret_from_path.side_effect = lambda path, secret_key: "mock_secret" if "client_id" in path else "mock_secret2"

    # Use the correct import path to patch IamClient
    with patch('credentials_utils.IamClient', return_value=mock_iam_instance):
        env = 'prod'
        creds = get_cli_creds("test_chamber", env)

    assert creds['client_id'] == 'mock_secret'
    assert creds['client_secret'] == 'mock_secret2'

def test_get_cli_creds_config_not_found(mock_load_config, mock_logging):
    # Mock the load_config to return None
    mock_load_config.return_value = None
    
    # Test for ValueError
    env = 'qa'
    with pytest.raises(ValueError, match=f"Configuration could not be loaded for environment: {env}"):
        get_cli_creds("test_chamber", env)

if __name__ == "__main__":
    pytest.main([__file__])
