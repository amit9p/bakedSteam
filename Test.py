

import yaml
from unittest.mock import patch, mock_open
from config_reader import config_read

def test_load_config_success():
    # Direct dictionary to mimic the YAML structure
    expected_dict = {
        'dev': {
            'VAULT_ROLE': '02f424c0-3b07-40cb-b6c0-73a732204e133',
            'LOCKBOX_ID': '5c9969ea-0b05-467c-8dde-09995ea5c70f'
        }
    }

    # Use mock_open with a simple YAML string
    valid_yaml_content = "dev:\n  VAULT_ROLE: '02f424c0-3b07-40cb-b6c0-73a732204e133'\n  LOCKBOX_ID: '5c9969ea-0b05-467c-8dde-09995ea5c70f'"
    m = mock_open(read_data=valid_yaml_content)
    with patch("builtins.open", m):
        with patch("yaml.safe_load", return_value=expected_dict):
            config = config_read.load_config("dev")
            print("Loaded config:", config)  # Debug print the loaded config
            assert config is not None
            assert 'dev' in config
            assert config['dev']['VAULT_ROLE'] == '02f424c0-3b07-40cb-b6c0-73a732204e133'
