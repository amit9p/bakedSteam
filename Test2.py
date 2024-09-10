
import pytest
from unittest.mock import patch, MagicMock

# Assuming the module is called `batch_module` and batch_process is within it.
# Replace `batch_module` with the correct module name.

@patch('batch_module.setup_turing_config')
@patch('batch_module.TuringPySparkClient')
def test_batch_process(mock_turing_client, mock_setup_turing_config):
    # Mock setup_turing_config to return a mock object
    mock_turing_obj = MagicMock()
    mock_setup_turing_config.return_value = mock_turing_obj
    
    # Instead of mocking DefaultAdapter class, we mock only its behavior after instantiation
    with patch('batch_module.DefaultAdapter') as MockAdapter:
        mock_adapter_instance = MockAdapter.return_value
        
        # Mock the TuringPySparkClient and its process method
        mock_client = MagicMock()
        mock_turing_client.return_value = mock_client
        mock_client.process.return_value = "mocked_dataframe"
        
        # Test DataFrame and parameters
        mock_df = MagicMock()
        dev_creds = "mock_dev_creds"
        env = "mock_env"
        
        # Test the case for "USTAXID"
        result = batch_module.batch_process(mock_df, dev_creds, env, tokenization="USTAXID")
        
        # Assertions for "USTAXID"
        mock_setup_turing_config.assert_called_once_with(dev_creds, env, "USTAXID")
        MockAdapter.assert_called_once_with(batch_module.TuringScope.USTAXID, input_column="formatted", TuringOperation=batch_module.TuringOperation.TOKENIZE)
        mock_client.process.assert_called_once_with(mock_df, mock_adapter_instance)
        
        assert result == "mocked_dataframe"
        
        # Reset mocks to test "PAN" case
        mock_setup_turing_config.reset_mock()
        MockAdapter.reset_mock()
        mock_client.process.reset_mock()
        
        # Test the case for "PAN"
        result = batch_module.batch_process(mock_df, dev_creds, env, tokenization="PAN")
        
        # Assertions for "PAN"
        mock_setup_turing_config.assert_called_once_with(dev_creds, env, "PAN")
        MockAdapter.assert_called_once_with(batch_module.TuringScope.PAN, input_column="formatted", TuringOperation=batch_module.TuringOperation.TOKENIZE)
        mock_client.process.assert_called_once_with(mock_df, mock_adapter_instance)
        
        assert result == "mocked_dataframe"
