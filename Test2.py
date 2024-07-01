
import pytest
from unittest.mock import patch, Mock
from utils.credentials_utils import get_cli_creds

@patch("utils.config_reader.load_config")
@patch("secret_sauce.IamClient")
def test_get_cli_creds_success_non_qa(mock_iam_client_class, mock_load_config):
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

    # Create a mock instance for IamClient and its methods
    mock_iam_client = Mock()
    mock_iam_client.get_secret_from_path.side_effect = lambda path, secret_key: f"{secret_key}_value"
    
    # Mock additional methods to prevent real network calls and errors
    mock_iam_client.get_token = Mock(return_value="mock_token")
    mock_iam_client.request_vault = Mock(return_value="mock_vault_response")

    # Ensure that initializing IamClient doesn't trigger real network calls
    mock_iam_client_class.return_value = mock_iam_client

    env = "prod"

    result = get_cli_creds("chamber_config", env)

    mock_load_config.assert_called_once_with("CONFIGS_CHAMBER", env)
    mock_iam_client_class.assert_called_once_with(domain="https://example.com", role="vault_role", lockbox_id="lockbox_id")
    mock_iam_client.get_secret_from_path.assert_any_call(path="client_id_path", secret_key="client_id")
    mock_iam_client.get_secret_from_path.assert_any_call(path="client_secret_path", secret_key="client_secret")
    assert result == {
        "client_id": "client_id_value",
        "client_secret": "client_secret_value"
    }

if __name__ == "__main__":
    pytest.main()
