

import unittest
from unittest.mock import patch, mock_open
import yaml
import os
from utils.config_reader import load_config

class TestConfigReader(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data="""
chamber:
  dev:
    VAULT_ROLE: test_vault_role_dev
    ENV: dev
stream:
  daily_accounts:
    dev: test_daily_accounts_dev
onelake_dataset:
  dev:
    daily_accounts: test_onelake_daily_accounts_dev
    """)
    def test_load_config(self, mock_file):
        # Using a relative path for the test
        config_path = os.path.join(os.path.dirname(__file__), '../config/app_config.yaml')
        with patch('os.path.join', return_value=config_path):
            config = load_config('dev')
        
        expected_env_config = {
            'VAULT_ROLE': 'test_vault_role_dev',
            'ENV': 'dev'
        }
        expected_stream_config = {
            'daily_accounts': {
                'dev': 'test_daily_accounts_dev'
            }
        }
        expected_onelake_dataset_config = {
            'daily_accounts': 'test_onelake_daily_accounts_dev'
        }

        self.assertEqual(config['env_config'], expected_env_config)
        self.assertEqual(config['stream_config'], expected_stream_config)
        self.assertEqual(config['onelake_dataset_config'], expected_onelake_dataset_config)

    @patch("builtins.open", new_callable=mock_open, read_data="invalid_yaml")
    def test_load_config_invalid_yaml(self, mock_file):
        config_path = os.path.join(os.path.dirname(__file__), '../config/app_config.yaml')
        with patch('os.path.join', return_value=config_path):
            config = load_config('dev')
        self.assertIsNone(config)

    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_load_config_file_not_found(self, mock_file):
        config_path = os.path.join(os.path.dirname(__file__), '../config/app_config.yaml')
        with patch('os.path.join', return_value=config_path):
            config = load_config('dev')
        self.assertIsNone(config)

if __name__ == '__main__':
    unittest.main()



______________
import yaml
import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def load_config(env: str, config_path: str = None):
    if config_path is None:
        # Default to an absolute path
        base_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_path, '../config/app_config.yaml')
    
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        return None
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        return None

    try:
        env_config = config.get('chamber', {}).get(env, {})
        stream_config = config.get('stream', {})
        onelake_dataset_config = config.get('onelake_dataset', {})
    except Exception as e:
        logger.error(f"Error accessing configuration for environment '{env}': {e}")
        return None

    return {
        'env_config': env_config,
        'stream_config': stream_config,
        'onelake_dataset_config': onelake_dataset_config.get(env, {}),
    }

