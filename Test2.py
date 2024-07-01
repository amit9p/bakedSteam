

import pytest
from unittest.mock import patch, Mock
from utils.credentials_utils import get_cli_creds

@patch("utils.config_reader.load_config")
@patch("secret_sauce.IamClient")
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

    # The expected result should match what the function returns for this input
    assert result == {
        "client_id": "CLIENTID",
        "client_secret": "CLIENTSECRET"
    }

if __name__ == "__main__":
    pytest.main()
