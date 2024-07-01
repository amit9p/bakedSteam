
from unittest.mock import Mock, patch

# Patching the relevant modules and methods
@patch('utils.config_reader.load_config', return_value=None)
@patch('requests.post')
@patch('botocore.auth.SigV4Auth.add_auth', return_value=None)
@patch('secret_sauce.IamClient')
def test_get_cli_creds_success_non_qa(mock_iam_client_class, mock_add_auth, mock_post, mock_load_config):
    # Define mock token
    mock_token = "mock_token"
    
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
    
    # Mock response for requests.post
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"auth": {"client_token": mock_token}}
    mock_post.return_value = mock_response

    # Create a mock instance for IamClient and its methods
    mock_iam_client = Mock()
    mock_iam_client.get_secret_from_path.side_effect = lambda path, secret_key: f"{secret_key}_value"
    mock_iam_client.get_token.return_value = mock_token
    mock_iam_client.request_vault.return_value = {"auth": {"client_token": mock_token}}
    mock_iam_client_class.return_value = mock_iam_client

    # Ensure that initializing IamClient doesn't trigger real network calls
    mock_iam_client_class.return_value = mock_iam_client

    env = "prod"
    result = get_cli_creds(chamber_config=mock_chamber_config, env=env)

    assert result == {
        "client_id": "client_id_value",
        "client_secret": "client_secret_value"
    }

    mock_load_config.assert_called_once_with("CONFIGS_CHAMBER")
    mock_iam_client_class.assert_called_once_with(domain="https://example.com", role="vault_role", lockbox_id="lockbox_id")
    mock_iam_client.get_secret_from_path.assert_any_call(path="client_id_path", secret_key="client_id")
    mock_iam_client.get_secret_from_path.assert_any_call(path="client_secret_path", secret_key="client_secret")
