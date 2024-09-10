
import pytest
from unittest.mock import patch, MagicMock

# Assuming the module is called `batch_module` and batch_process is within it.
# Replace `batch_module` with the correct module name.

@patch('batch_module.setup_turing_config')
@patch('batch_module.TuringPySparkClient')
def test_batch_process(mock_turing_client, mock_setup_turing_config):
    # Mock setup_turing_config to return a mock object with necessary properties
    mock_turing_obj = MagicMock()
    mock_turing_obj.configure_mock(api_npi_url="mock_npi_url", api_pci_url="mock_pci_url")
    mock_setup_turing_config.return_value = mock_turing_obj
    
    # Now, do not mock the class itself. Create an instance but mock only the methods.
    with patch('batch_module.DefaultAdapter.__init__', return_value=None):
        # Mock the methods you are going to use
        with patch('batch_module.DefaultAdapter.read_row', return_value="mock_read_row"), \
             patch('batch_module.DefaultAdapter.update_row', return_value="mock_update_row"):
            
            # Mock TuringPySparkClient and its process method
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
            mock_client.process.assert_called_once_with(mock_df, MagicMock())
            
            assert result == "mocked_dataframe"
            
            # Reset mocks to test "PAN" case
            mock_setup_turing_config.reset_mock()
            mock_client.process.reset_mock()
            
            # Test the case for "PAN"
            result = batch_module.batch_process(mock_df, dev_creds, env, tokenization="PAN")
            
            # Assertions for "PAN"
            mock_setup_turing_config.assert_called_once_with(dev_creds, env, "PAN")
            mock_client.process.assert_called_once_with(mock_df, MagicMock())
            
            assert result == "mocked_dataframe"
####
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
    with patch('batch_module.DefaultAdapter.__init__', return_value=None):
        # Mock the methods you are going to use
        with patch('batch_module.DefaultAdapter.read_row', return_value="mock_read_row"), \
             patch('batch_module.DefaultAdapter.update_row', return_value="mock_update_row"):
            
            # Mock TuringPySparkClient and its process method
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
            mock_client.process.assert_called_once_with(mock_df, MagicMock())
            
            assert result == "mocked_dataframe"
            
            # Reset mocks to test "PAN" case
            mock_setup_turing_config.reset_mock()
            mock_client.process.reset_mock()
            
            # Test the case for "PAN"
            result = batch_module.batch_process(mock_df, dev_creds, env, tokenization="PAN")
            
            # Assertions for "PAN"
            mock_setup_turing_config.assert_called_once_with(dev_creds, env, "PAN")
            mock_client.process.assert_called_once_with(mock_df, MagicMock())
            
            assert result == "mocked_dataframe"
#########
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
    
    # Now, do not mock the class itself. Create an instance but mock only the methods.
    # We mock the DefaultAdapter's behavior after instantiation rather than the class definition.
    with patch('batch_module.DefaultAdapter.__init__', return_value=None):
        with patch('batch_module.DefaultAdapter.some_method', return_value=MagicMock()) as mock_adapter_method:
            
            # Mock TuringPySparkClient and its process method
            mock_client = MagicMock
