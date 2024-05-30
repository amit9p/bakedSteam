

import unittest
from unittest.mock import patch, mock_open
import yaml
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
        config = load_config('dev')
        self.assertIsNone(config)

    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_load_config_file_not_found(self, mock_file):
        config = load_config('dev')
        self.assertIsNone(config)

if __name__ == '__main__':
    unittest.main()


-‐---------

import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from ecbr_assembler.assembler.core import Assembler

class TestAssembler(unittest.TestCase):

    @patch('utils.config_reader.load_config')
    def test_run(self, mock_load_config):
        mock_load_config.return_value = {
            'env_config': {
                'VAULT_ROLE': 'test_vault_role_dev',
                'ENV': 'dev'
            },
            'stream_config': {
                'daily_accounts': {
                    'dev': 'test_daily_accounts_dev'
                }
            },
            'onelake_dataset_config': {
                'daily_accounts': 'test_onelake_daily_accounts_dev'
            }
        }

        spark_session = SparkSession.builder.master("local").appName("test").getOrCreate()
        assembler = Assembler(spark_session)

        with patch.object(assembler, 'read_whole_parquet_file', return_value=None) as mock_read:
            assembler.run('dev', 'dataset_id', 'business_dt', 'run_id', 'ALL')
            mock_read.assert_not_called()  # Replace with actual method calls you expect

    @patch('utils.config_reader.load_config', return_value=None)
    def test_run_config_not_loaded(self, mock_load_config):
        spark_session = SparkSession.builder.master("local").appName("test").getOrCreate()
        assembler = Assembler(spark_session)

        with self.assertLogs('ecbr_assembler.assembler.core', level='ERROR') as cm:
            assembler.run('dev', 'dataset_id', 'business_dt', 'run_id', 'ALL')
            self.assertIn('Configuration could not be loaded for environment: dev', cm.output[0])

if __name__ == '__main__':
    unittest.main()
