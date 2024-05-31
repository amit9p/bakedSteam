
# config_reader.py
import yaml
import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def load_config(env: str, config_path: str = None):
    if config_path is None:
        base_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_path, '../config/app_config.yaml')

    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logger.error(f'Configuration file not found: {config_path}')
        return None
    except yaml.YAMLError as e:
        logger.error(f'Error parsing YAML file: {e}')
        return None

    try:
        env_config = config.get('chamber', {}).get(env, {})
        stream_config = config.get('stream', {}).get(env, {})
        onelake_dataset_config = config.get('onelake_dataset', {}).get(env, {})
    except Exception as e:
        logger.error(f'Error accessing configuration for environment {env}: {e}')
        return None

    return {
        'env_config': env_config,
        'stream_config': stream_config,
        'onelake_dataset_config': onelake_dataset_config,
    }


____________

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
        assert mock_logger.call_count == 1
        assert "Error accessing configuration for environment dev: 'NoneType' object has no attribute 'get'" in mock_logger.call_args[0][0]

# Running the tests
if __name__ == "__main__":
    pytest.main()
