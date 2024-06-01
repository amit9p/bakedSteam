
import pytest
from unittest.mock import patch, mock_open
from config_reader import config_read

def test_load_config_success():
    # Example YAML content
    yaml_content = """
env_config: QA config data
stream_config: QA stream data
oneLake_dataset_config: QA dataset data
"""
    # Expected structure from yaml.safe_load
    expected_config = {
        'env_config': 'QA config data',
        'stream_config': 'QA stream data',
        'oneLake_dataset_config': 'QA dataset data'
    }

    with patch("builtins.open", mock_open(read_data=yaml_content), create=True) as mocked_file:
        with patch("yaml.safe_load", return_value=expected_config) as mocked_yaml:
            # Call the load_config function
            config = config_read.load_config("dev")
            print("Config Loaded:", config)  # Debug output

            # Assertions to verify the functionality
            assert config is not None
            assert mocked_file.called  # Ensure the file was attempted to be opened
            assert mocked_yaml.called  # Ensure yaml.safe_load was called
            assert config['env_config'] == 'QA config data'
            assert config['stream_config'] == 'QA stream data'
            assert config['oneLake_dataset_config'] == 'QA dataset data'
