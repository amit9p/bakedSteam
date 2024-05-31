
# test_config_reader.py
import pytest
from unittest.mock import patch, mock_open
import logging
from config_reader import load_config

# Mock data
valid_yaml_content = """
chamber:
  dev:
    key: value
stream:
  dev:
    key: value
onelake_dataset:
  dev:
    key: value
"""

invalid_yaml_content = """
chamber:
  dev
    key: value
"""

@pytest.fixture
def mock_logger():
    logger = logging.getLogger('config_reader')
    with patch.object(logger, 'error') as mock_log_error:
        yield mock_log_error

def test_load_config_valid(mock_logger):
    with patch('builtins.open', mock_open(read_data=valid_yaml_content)), \
         patch('os.path.join', return_value='config/app_config.yaml'):
        config = load_config('dev')
        assert config['env_config']['key'] == 'value'
        assert config['stream_config']['key'] == 'value'
        assert config['onelake_dataset_config']['key'] == 'value'

def test_load_config_file_not_found(mock_logger):
    with patch('builtins.open', side_effect=FileNotFoundError), \
         patch('os.path.join', return_value='config/app_config.yaml'):
        config = load_config('dev')
        assert config is None
        mock_logger.assert_called_once_with('Configuration file not found: config/app_config.yaml')

def test_load_config_yaml_error(mock_logger):
    with patch('builtins.open', mock_open(read_data=invalid_yaml_content)), \
         patch('os.path.join', return_value='config/app_config.yaml'):
        config = load_config('dev')
        assert config is None
        assert mock_logger.call_count == 1
        assert "Error parsing YAML file" in mock_logger.call_args[0][0]

def test_load_config_missing_keys(mock_logger):
    incomplete_yaml_content = """
    chamber:
      dev:
        key: value
    """
    with patch('builtins.open', mock_open(read_data=incomplete_yaml_content)), \
         patch('os.path.join', return_value='config/app_config.yaml'):
        config = load_config('dev')
        assert config['env_config']['key'] == 'value'
        assert config['stream_config'] is None
        assert config['onelake_dataset_config'] is None
        mock_logger.assert_called_once_with("Error accessing configuration for environment dev: 'NoneType' object has no attribute 'get'")

# Running the tests
if __name__ == "__main__":
    pytest.main()
