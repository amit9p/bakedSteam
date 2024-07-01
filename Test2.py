

from unittest.mock import Mock, patch
import pytest

@patch('utils.config_reader.load_config', return_value=None)
@patch('secret_sauce.IamClient')
def test_get_cli_creds_exception(mock_iam_client_class, mock_load_config):
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
    env = "prod"

    with patch('ecbr_logging.logging') as mock_logger:
        with pytest.raises(Exception):
            get_cli_creds(chamber_config=mock_chamber_config, env=env)
        mock_logger.error.assert_called_once_with("test exception")

if __name__ == "__main__":
    pytest.main()
