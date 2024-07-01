import pytest
from unittest.mock import patch, Mock
from your_module import get_cli_creds  # Replace 'your_module' with the actual module name

@patch("your_module.load_config")
@patch("your_module.IamClient")
def test_get_cli_creds_success_qa(mock_iam_client_class, mock_load_config):
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
    env = "qa"

    result = get_cli_creds("chamber_config", env)

    assert result == {
        "client_id": "CLIENTID",
        "client_secret": "CLIENTSECRET"
    }

@patch("your_module.load_config")
@patch("your_module.IamClient")
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
    mock_iam_client = Mock()
    mock_iam_client.get_secret_from_path.side_effect = lambda path, secret_key: f"{secret_key}_value"
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

@patch("your_module.load_config", return_value=None)
def test_get_cli_creds_no_config(mock_load_config):
    # Mock the necessary objects and their methods
    env = "prod"

    with pytest.raises(ValueError) as excinfo:
        get_cli_creds("chamber_config", env)

    assert str(excinfo.value) == "Configuration could not be loaded for environment: prod"

@patch("your_module.load_config")
@patch("your_module.IamClient")
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

    with patch("your_module.logger.error") as mock_logger_error:
        with pytest.raises(Exception):
            get_cli_creds("chamber_config", env)
        mock_logger_error.assert_called_once_with("test exception")

if __name__ == "__main__":
    pytest.main()
