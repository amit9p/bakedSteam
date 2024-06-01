
import pytest
from unittest.mock import mock_open, patch
from config_reader import config_read

# Sample data for tests
valid_yaml = "key: value"
invalid_yaml = "key: value:"

def test_load_config_success():
    """ Test load_config method for successfully loading configuration. """
    with patch("builtins.open", mock_open(read_data=valid_yaml)):
        with patch("os.path.join", return_value="fake_path/app_config.yaml"):
            config = config_read.load_config()
            assert config is not None
            assert config['key'] == 'value'

def test_load_config_file_not_found():
    """ Test load_config method when the configuration file is not found. """
    with patch("builtins.open", side_effect=FileNotFoundError()):
        with patch("os.path.join", return_value="fake_path/app_config.yaml"):
            config = config_read.load_config()
            assert config is None

def test_load_config_yaml_error():
    """ Test load_config method when there is a YAML parsing error. """
    with patch("builtins.open", mock_open(read_data=invalid_yaml)):
        with patch("os.path.join", return_value="fake_path/app_config.yaml"):
            config = config_read.load_config()
            assert config is None

# Optionally, add more tests to cover other scenarios or exceptions
