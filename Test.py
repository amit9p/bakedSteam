

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

    @patch('pyspark.sql.SparkSession.read')
    def test_read_whole_parquet_file(self, mock_read):
        mock_spark = MagicMock(SparkSession)
        assembler = Assembler(mock_spark)
        mock_df = MagicMock()
        mock_read.parquet.return_value = mock_df

        result = assembler.read_whole_parquet_file("dummy_path")

        mock_read.parquet.assert_called_once_with("dummy_path")
        self.assertEqual(result, mock_df)

if __name__ == '__main__':
    unittest.main()
