

import pytest
from unittest.mock import patch, mock_open
from config_reader import config_read

def test_load_config_success():
    # Mock the open function to return the YAML data
    yaml_content = """
env_config: QA config data
stream_config: QA stream data
oneLake_dataset_config: QA dataset data
"""
    expected_config = {
        'env_config': 'QA config data',
        'stream_config': 'QA stream data',
        'oneLake_dataset_config': 'QA dataset data'
    }
    
    with patch("builtins.open", mock_open(read_data=yaml_content), create=True):
        with patch("yaml.safe_load", return_value=expected_config):
            # Load configuration
            config = config_read.load_config()
            assert config is not None
            assert config['env_config'] == 'QA config data'
            assert config['stream_config'] == 'QA stream data'
            assert config['oneLake_dataset_config'] == 'QA dataset data'
