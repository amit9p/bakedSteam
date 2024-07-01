

import pytest
from unittest.mock import patch, Mock
from ecbr_assembler.credentials_utils import get_cli_creds

@patch('utils.config_reader.load_config')
@patch('secret_sauce.IamClient')
@patch('ecbr_assembler.credentials_utils.logger')  # Patch the logger used in credentials_utils
def test_get_cli_creds_exception(mock_logger, mock_iam_client_class, mock_load_config):
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
    mock_iam_client_class.side_effect = ValueError("test exception")
    
    # Create a mock logger
    mock_logger_instance = Mock()
    mock_logger.return_value = mock_logger_instance
    
    env = "prod"
    
    with pytest.raises(ValueError):
        get_cli_creds(chamber_config=mock_chamber_config, env=env)
    
    # Ensure the error was logged
    mock_logger_instance.error.assert_called_once_with("test exception")

if __name__ == "__main__":
    pytest.main()
