

import pytest
from unittest.mock import patch, MagicMock

# Mock data for testing
mock_df = MagicMock()  # This could represent a Spark DataFrame
mock_dev_creds = {'client_id': 'mock_client_id', 'client_secret': 'mock_secret'}
mock_env = "qa"
mock_tokenization = "USTAXID"

# Test for setup_turing_config
@patch('your_module.setup_turing_config')
def test_setup_turing_config(mock_setup):
    # Mock the return value of setup_turing_config
    mock_setup.return_value = {
        'TURING_API_OAUTH_URL': 'mock_url',
        'TURING_OAUTH_CLIENT_ID': 'mock_client_id',
        'TURING_OAUTH_CLIENT_SECRET': 'mock_secret',
        'TURING_CLIENT_SSL_VERIFY': False
    }
    
    result = setup_turing_config(mock_dev_creds, mock_env, mock_tokenization)
    assert result['TURING_API_OAUTH_URL'] == 'mock_url'
    assert result['TURING_OAUTH_CLIENT_ID'] == 'mock_client_id'

# Test for batch_process
@patch('your_module.TuringPySparkClient.process')
@patch('your_module.DefaultAdapter')
def test_batch_process(mock_adapter, mock_process):
    # Mock the processing method of the TuringPySparkClient
    mock_process.return_value = "mocked_processed_df"
    
    # Mock adapter
    mock_adapter_instance = mock_adapter.return_value
    
    # Call the batch_process function
    result = batch_process(mock_df, mock_dev_creds, mock_env, mock_tokenization)
    
    assert result == "mocked_processed_df"

# Test for assembler_etl
@patch('your_module.getResolvedOptions')
@patch('your_module.GlueContext')
@patch('your_module.SparkContext')
@patch('your_module.read_parquet_file')  # Mocking the read_parquet_file
@patch('your_module.batch_process')  # Mocking the batch_process method
def test_assembler_etl(mock_batch_process, mock_read_parquet_file, mock_spark_context, mock_glue_context, mock_get_resolved_options):
    # Mock the getResolvedOptions to return necessary arguments
    mock_get_resolved_options.return_value = {
        'input_s3_path': 'mock_input_path',
        'output_s3_path': 'mock_output_path',
        'env': 'qa',
        'client_id': 'mock_client_id',
        'client_secret': 'mock_secret'
    }
    
    # Mock the Spark and Glue contexts
    mock_spark_context.return_value = MagicMock()
    mock_glue_context.return_value = MagicMock()

    # Mock read_parquet_file return value
    mock_read_parquet_file.return_value = "mocked_dataframe"

    # Mocking batch_process
    mock_batch_process.side_effect = ["mocked_tokenized_ustaxid_df", "mocked_tokenized_pan_df"]
    
    # Call the assembler_etl method
    assembler_etl()

    # Assert that read_parquet_file was called with the correct arguments
    mock_read_parquet_file.assert_called_with(mock_spark_context.return_value, 'mock_input_path')

    # Ensure batch_process is called correctly
    mock_batch_process.assert_any_call(mock.ANY, mock_dev_creds, mock_env, "USTAXID")
    mock_batch_process.assert_any_call(mock.ANY, mock_dev_creds, mock_env, "PAN")

    # Optionally check that the final output is written
    mock_glue_context.return_value.write_parquet.assert_called_once_with(
        "mocked_dataframe", 'mock_output_path'
    )
