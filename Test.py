

import yaml
from unittest.mock import patch, mock_open
from config_reader import config_read

def test_load_config_success():
    """Test load_config method for successfully loading configuration from a YAML file."""
    # Simulated content of the YAML file as seen in the screenshots
    valid_yaml_content = """
    dev:
        VAULT_ROLE: 02f424c0
        LOCKBOX_ID: 5c9969ea
        CHAMBER_URL: https://chamber-qa.cloudtgt.capitalone.com
    """
    expected_dict = {
        'dev': {
            'VAULT_ROLE': '02f424c0',
            'LOCKBOX_ID': '5c9969ea',
            'CHAMBER_URL': 'https://chamber-qa.cloudtgt.capitalone.com'
        }
    }
    
    # Setup the mock to return this content
    m = mock_open(read_data=valid_yaml_content)
    
    with patch("builtins.open", m, create=True):
        with patch("os.path.join", return_value="/fake/path/app_config.yaml"):
            with patch("os.path.abspath", return_value="/fake/path"):
                with patch("yaml.safe_load", return_value=expected_dict):
                    config = config_read.load_config("dev")
                    print("Loaded config:", config)  # Debug print to check what is loaded
                    assert config is not None
                    assert config['dev']['VAULT_ROLE'] == '02f424c0-3b07-40cb-b6c0-73a732204e133'  # Access nested key

    # Ensure the file was opened correctly
    m.assert_called_once_with("/fake/path/app_config.yaml", 'r')
