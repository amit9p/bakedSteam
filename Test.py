
import pytest
from unittest.mock import patch, MagicMock
from ecb_assembler.assembler.core import Assembler

@pytest.fixture
def assembler():
    # Mock the SparkSession object
    spark_session = MagicMock()
    return Assembler(spark_session)

@patch('utils.config_reader.load_config')
@patch('ecb_assembler.assembler.core.SparkSession')
def test_run_method(mock_spark_session, mock_load_config, assembler):
    # Mock the load_config method to return a dummy config
    mock_load_config.return_value = {
        'env_config': {'key': 'value'},
        'stream_config': {'key': 'value'},
        'onelake_dataset_config': {'key': 'value'}
    }

    # Call the run method with necessary kwargs
    assembler.run(env='test_env', dataset_id='test_dataset', business_dt='2024-05-30', run_id='test_run')

    # Assertions to ensure the load_config was called correctly
    mock_load_config.assert_called_once_with('test_env')

    # Additional assertions can be added here based on what the run method is supposed to do
    # For example:
    # assert assembler.some_internal_state == expected_value

if __name__ == "__main__":
    pytest.main()
