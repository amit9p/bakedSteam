

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
