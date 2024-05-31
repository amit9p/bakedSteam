

import pytest
from unittest.mock import patch, MagicMock
from ecb_assembler.assembler.core import Assembler
import logging

@pytest.fixture
def assembler():
    # Mock the SparkSession object
    spark_session = MagicMock()
    return Assembler(spark_session)

def test_run_method(assembler):
    with patch('utils.config_reader.load_config') as mock_load_config, \
         patch('pyspark.sql.SparkSession'):
        # Mock the load_config method to return a dummy config
        mock_load_config.return_value = {
            'env_config': {'key': 'value'},
            'stream_config': {'key': 'value'},
            'onelake_dataset_config': {'key': 'value'}
        }

        # Adding debug logs
        logging.basicConfig(level=logging.DEBUG)
        logging.debug("Starting test for run method")

        # Call the run method with necessary kwargs
        assembler.run(env='test_env', dataset_id='test_dataset', business_dt='2024-05-30', run_id='test_run')

        logging.debug("Run method executed")

        # Assertions to ensure the load_config was called correctly
        try:
            mock_load_config.assert_called_once_with('test_env')
            logging.debug("load_config was called once with 'test_env'")
        except AssertionError as e:
            logging.error(f"AssertionError: {e}")
            raise

if __name__ == "__main__":
    pytest.main()
