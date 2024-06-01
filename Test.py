

import yaml
from unittest.mock import patch, mock_open, MagicMock
from config_reader import config_read

def test_load_config_success():
    """Test load_config method for successfully loading configuration."""
    # Prepare the YAML content and simulate the file content
    valid_yaml = "key: value"
    mocked_file = mock_open(read_data=valid_yaml)
    with patch("builtins.open", mocked_file, create=True):
        with patch("os.path.join", return_value="fake_path/app_config.yaml"):
            with patch("os.path.abspath", return_value="fake_path"):
                # Mock yaml.safe_load if your config reader uses it
                with patch('yaml.safe_load', return_value={'key': 'value'}):
                    config = config_read.load_config("dev")  # Assuming 'dev' is a valid input
                    assert config is not None
                    assert config['key'] == 'value'

    # Ensure the file was opened correctly
    mocked_file.assert_called_once_with("fake_path/app_config.yaml", 'r')
