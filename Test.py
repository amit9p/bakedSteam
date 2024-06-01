

import pytest
from unittest.mock import patch, mock_open
from config_reader import config_read

def test_load_config_success():
    # This is the minimal expected YAML content as a string
    yaml_content = """
env_config: QA config data
stream_config: QA stream data
oneLake_dataset_config: QA dataset data
"""
    # Expected dictionary that mirrors the YAML structure
    expected_config = {
        'env_config': 'QA config data',
        'stream_config': 'QA stream data',
        'oneLake_dataset_config': 'QA dataset data'
    }

    # Mock the open function to simulate reading the above YAML content
    with patch("builtins.open", mock_open(read_data=yaml_content), create=True) as mocked_file:
        # Mock yaml.safe_load to directly return the expected configuration
        with patch("yaml.safe_load", return_value=expected_config) as mocked_yaml:
            # Execute the load_config function which we're testing
            config = config_read.load_config()
            print("Loaded config:", config)  # Output the result for debugging

            # Assertions to check the function's effectiveness
            assert mocked_file.called, "File open not called"
            assert mocked_yaml.called, "YAML load not called"
            assert config is not None, "Configuration is None"
            assert config['env_config'] == 'QA config data', "env_config not loaded correctly"
            assert config['stream_config'] == 'QA stream data', "stream_config not loaded correctly"
            assert config['oneLake_dataset_config'] == 'QA dataset data', "oneLake_dataset_config not loaded correctly"
