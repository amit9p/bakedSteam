

import pytest
from unittest.mock import patch, Mock
from your_module import create_turing_df  # Replace 'your_module' with the actual module name

@patch("your_module.setup_turing_config")
@patch("your_module.TuringPySparkClient")
def test_create_turing_df_ustaxid(mock_turing_client_class, mock_setup_turing_config):
    # Mock the necessary objects and their methods
    cli_cred = Mock()
    df = Mock()
    env = "test_env"
    token_type = "USTAXID"
    mock_turing_client = Mock()
    mock_turing_client.process.return_value = "processed_df"
    mock_turing_client_class.return_value = mock_turing_client
    mock_setup_turing_config.return_value = "turing_config"

    result = create_turing_df(cli_cred, df, env, token_type)

    mock_setup_turing_config.assert_called_once_with(cli_cred, env)
    mock_turing_client_class.assert_called_once_with("turing_config")
    mock_turing_client.process.assert_called_once()
    assert result == "processed_df"

@patch("your_module.setup_turing_config")
@patch("your_module.TuringPySparkClient")
def test_create_turing_df_pan(mock_turing_client_class, mock_setup_turing_config):
    # Mock the necessary objects and their methods
    cli_cred = Mock()
    df = Mock()
    env = "test_env"
    token_type = "PAN"
    mock_turing_client = Mock()
    mock_turing_client_class.return_value = mock_turing_client
    mock_setup_turing_config.return_value = "turing_config"

    result = create_turing_df(cli_cred, df, env, token_type)

    mock_setup_turing_config.assert_called_once_with(cli_cred, env)
    mock_turing_client_class.assert_called_once_with("turing_config")
    assert result == df

@patch("your_module.setup_turing_config")
@patch("your_module.TuringPySparkClient")
def test_create_turing_df_exception(mock_turing_client_class, mock_setup_turing_config):
    # Mock the necessary objects and their methods
    cli_cred = Mock()
    df = Mock()
    env = "test_env"
    token_type = "USTAXID"
    mock_turing_client = Mock()
    mock_turing_client.process.side_effect = Exception("test exception")
    mock_turing_client_class.return_value = mock_turing_client
    mock_setup_turing_config.return_value = "turing_config"

    with patch("your_module.logger.error") as mock_logger_error:
        result = create_turing_df(cli_cred, df, env, token_type)
        mock_logger_error.assert_called_once_with("test exception")

    assert result is None

if __name__ == "__main__":
    pytest.main()
