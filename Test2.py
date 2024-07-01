
import pytest
import yaml
import os
from unittest.mock import patch, mock_open
from config_reader import load_config

# Sample config data for testing
config_data = """
chamber:
  dev:
    VAULT_ROLE: 02f42e4e-3b07-40cb-b6c0-73a737224e133
    LOCKBOX_ID: 95c9966a-0b05-467c-8ddc-89905e5ca70f
    CHAMBER_URL: https://chamber-qa.clouddqt.capitalone.com
    CLIENT_ID_PATH: apps/ecbr-data-ingest/client_id
    CLIENT_SECRET_PATH: apps/ecbr-data-ingest/client_secret
    ENV: dev
    SDP_ENV: qa
    FILEPULL_IAM_ROLE: arn:aws:iam::592502317683:role/BAECBR/ASVSDP/OneStream

  qa:
    VAULT_ROLE: c0d9ab4d-50ec-4097-8044-cff0e295fbe9
    LOCKBOX_ID: 95c9966a-0b05-467c-8ddc-89905e5ca70f
    CHAMBER_URL: https://chamber-qa.clouddqt.capitalone.com
    CLIENT_ID_PATH: apps/ecbr-data-ingest/client_id
    CLIENT_SECRET_PATH: apps/ecbr-data-ingest/client_secret
    ENV: qa
    SDP_ENV: qa
    FILEPULL_IAM_ROLE: arn:aws:iam::592502317683:role/BAECBR/ASVSDP/OneStream

  cte:
    VAULT_ROLE: 085398d1-7f3d-49af-89c2-83593ea9de78
    LOCKBOX_ID: 2450f95d-b041-4359-bddb-70d433a10a7d
    CHAMBER_URL: https://chamber.cloud.capitalone.com
    CLIENT_ID_PATH: apps/ecbr-data-ingest/client_id
    CLIENT_SECRET_PATH: apps/ecbr-data-ingest/client_secret
    ENV: cte
    SDP_ENV: prod
    FILEPULL_IAM_ROLE: arn:aws:iam::1066088546225:role/BAECBR/ASVSDP/OneStream

  prod:
    VAULT_ROLE: 5df02813-5a95-47c0-b5d8-ac41fe521ab2
    LOCKBOX_ID: 2450f95d-b041-4359-bddb-70d433a10a7d
    CHAMBER_URL: https://chamber.cloud.capitalone.com
    CLIENT_ID_PATH: apps/ecbr-data-ingest/client_id
    CLIENT_SECRET_PATH: apps/ecbr-data-ingest/client_secret
    ENV: prod
    SDP_ENV: prod
    FILEPULL_IAM_ROLE: arn:aws:iam::1066088546225:role/BAECBR/ASVSDP/OneStream
"""

# Mock data to be used in tests
mock_config = yaml.safe_load(config_data)

@patch("builtins.open", new_callable=mock_open, read_data=config_data)
@patch("os.path.join", return_value="/config/app_config.yaml")
@patch("os.path.dirname", return_value="/")
def test_load_config_success(mock_dirname, mock_path_join, mock_file):
    # Testing successful config load
    config = load_config("chamber", "dev")
    assert config["VAULT_ROLE"] == "02f42e4e-3b07-40cb-b6c0-73a737224e133"
    assert config["LOCKBOX_ID"] == "95c9966a-0b05-467c-8ddc-89905e5ca70f"
    assert config["CHAMBER_URL"] == "https://chamber-qa.clouddqt.capitalone.com"
    assert config["CLIENT_ID_PATH"] == "apps/ecbr-data-ingest/client_id"
    assert config["CLIENT_SECRET_PATH"] == "apps/ecbr-data-ingest/client_secret"
    assert config["ENV"] == "dev"
    assert config["SDP_ENV"] == "qa"
    assert config["FILEPULL_IAM_ROLE"] == "arn:aws:iam::592502317683:role/BAECBR/ASVSDP/OneStream"

@patch("builtins.open", side_effect=FileNotFoundError)
@patch("os.path.join", return_value="/config/app_config.yaml")
@patch("os.path.dirname", return_value="/")
def test_load_config_file_not_found(mock_dirname, mock_path_join, mock_file):
    # Testing config load with file not found
    config = load_config("chamber", "dev")
    assert config is None

@patch("builtins.open", new_callable=mock_open, read_data="not a yaml file")
@patch("os.path.join", return_value="/config/app_config.yaml")
@patch("os.path.dirname", return_value="/")
def test_load_config_yaml_error(mock_dirname, mock_path_join, mock_file):
    # Testing config load with YAML parsing error
    config = load_config("chamber", "dev")
    assert config is None

@patch("builtins.open", new_callable=mock_open, read_data=config_data)
@patch("os.path.join", return_value="/config/app_config.yaml")
@patch("os.path.dirname", return_value="/")
def test_load_config_key_error(mock_dirname, mock_path_join, mock_file):
    # Testing config load with missing key
    config = load_config("chamber", "nonexistent")
    assert config is None

@patch("builtins.open", new_callable=mock_open, read_data=config_data)
@patch("os.path.join", return_value="/config/app_config.yaml")
@patch("os.path.dirname", return_value="/")
def test_load_config_env_key_error(mock_dirname, mock_path_join, mock_file):
    # Testing config load with missing env key
    config = load_config("chamber", "dev_nonexistent")
    assert config is None

if __name__ == "__main__":
    pytest.main()
