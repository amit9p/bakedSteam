

import pytest
from unittest.mock import patch, mock_open
import yaml

# Import the load_config method
from config_reader import load_config

# Sample YAML content for tests
sample_yaml_content = """
dev:
  VAULT_ROLE: dummy_vault_role
  LOCKBOX_ID: dummy_lockbox_id
  CHAMBER_URL: dummy_chamber_url
  CLIENT_ID_PATH: dummy_client_id_path
  CLIENT_SECRET_PATH: dummy_client_secret_path
  ENV: dev
  SDP_ENV: qa
"""

@pytest.fixture
def mock_open_yaml():
    with patch("builtins.open", mock_open(read_data=sample_yaml_content)) as mock_file:
        yield mock_file

@pytest.fixture
def mock_os_path():
    with patch("os.path.dirname", return_value="/path/to/config"):
        with patch("os.path.abspath", return_value="/path/to/config"):
            with patch("os.path.join", return_value="/path/to/config/app_config.yaml"):
                yield

def test_load_config_generic_exception(mock_open_yaml, mock_os_path):
    with patch("yaml.safe_load", side_effect=Exception):
        result = load_config("dev", "qa")
        assert result is None
