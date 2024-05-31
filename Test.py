

import pytest
from unittest.mock import patch, MagicMock
from ecb_assembler.assembler.core import Assembler
from ecb_assembler.assembler.config_reader import load_config

@pytest.fixture
def mock_logger():
    with patch('ecb_assembler.assembler.core.logger') as mock_log:
        yield mock_log

@pytest.fixture
def mock_load_config():
    with patch('ecb_assembler.assembler.core.load_config') as mock_load:
        yield mock_load

def test_run_valid_config(mock_logger, mock_load_config):
    # Mock configuration
    mock_load_config.return_value = {
        'env_config': {'VAULT_ROLE': 'test_role'},
        'stream_config': {'daily_accounts': {'dev': 'stream_config_value'}},
        'onelake_dataset_config': {'daily_accounts': 'onelake_config_value'}
    }

    # Mock SparkSession
    spark = MagicMock()

    # Create Assembler instance
    assembler = Assembler(spark)

    # Run method with kwargs
    assembler.run(
        env='dev',
        dataset_id='dataset_id',
        business_dt='2024-05-30',
        run_id='run_id',
        file_type='TU'
    )

    # Assertions
    mock_logger.info.assert_any_call("Loaded environment config for dev: {'VAULT_ROLE': 'test_role'}")
    mock_logger.info.assert_any_call("Loaded stream config for dev: {'daily_accounts': {'dev': 'stream_config_value'}}")
    mock_logger.info.assert_any_call("Loaded OneLake dataset config for dev: {'daily_accounts': 'onelake_config_value'}")
    mock_logger.info.assert_any_call("Vault Role for dev: test_role")
    mock_logger.info.assert_any_call("Daily Accounts Config for dev: stream_config_value")
    mock_logger.info.assert_any_call("OneLake Daily Accounts for dev: onelake_config_value")

def test_run_config_not_found(mock_logger, mock_load_config):
    # Mock configuration loading failure
    mock_load_config.return_value = None

    # Mock SparkSession
    spark = MagicMock()

    # Create Assembler instance
    assembler = Assembler(spark)

    # Run method with kwargs
    with pytest.raises(ValueError, match="Configuration could not be loaded for environment: dev"):
        assembler.run(
            env='dev',
            dataset_id='dataset_id',
            business_dt='2024-05-30',
            run_id='run_id',
            file_type='TU'
        )

    # Assertions
    mock_logger.error.assert_called_once_with("Configuration could not be loaded for environment: dev")

def test_run_exception_handling(mock_logger, mock_load_config):
    # Mock configuration to trigger exception
    mock_load_config.side_effect = Exception("Unexpected error")

    # Mock SparkSession
    spark = MagicMock()

    # Create Assembler instance
    assembler = Assembler(spark)

    # Run method with kwargs
    assembler.run(
        env='dev',
        dataset_id='dataset_id',
        business_dt='2024-05-30',
        run_id='run_id',
        file_type='TU'
    )

    # Assertions
    mock_logger.error.assert_called_once_with("Unexpected error")

# Running the tests
if __name__ == "__main__":
    pytest.main()
