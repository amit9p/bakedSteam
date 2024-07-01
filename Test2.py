

import pytest
from unittest.mock import patch, Mock

# Import the necessary components
from ecbr_assembler.credentials_utils import get_cli_creds

# Define the test case
@patch('utils.config_reader.load_config')
@patch('secret_sauce.IamClient')
@patch('ecbr_logging.getLogger')  # Correctly patching the logger
def test_get_cli_creds_exception(mock_get_logger, mock_iam_client_class, mock_load_config):
    # Mock the necessary objects and their methods
    mock_chamber_config = {
        "env_config": {
            "CHAMBER_URL": "https://example.com",
            "VAULT_ROLE": "vault_role",
            "LOCKBOX_ID": "lockbox_id",
            "CLIENT_ID_PATH": "client_id_path",
            "CLIENT_SECRET_PATH": "client_secret_path"
        }
    }
    
    mock_load_config.return_value = mock_chamber_config
    mock_iam_client_class.side_effect = Exception("test exception")
    
    # Create a mock logger
    mock_logger = Mock()
    mock_get_logger.return_value = mock_logger
    
    env = "prod"
    
    with pytest.raises(Exception):
        get_cli_creds(chamber_config=mock_chamber_config, env=env)
    
    # Ensure the error was logged
    mock_logger.error.assert_called_once_with("test exception")

if __name__ == "__main__":
    pytest.main()
