
import pytest
from utils.config_reader import load_config
from unittest.mock import mock_open, patch

# Test successful config load
@pytest.mark.parametrize("config_type, env", [("app", "dev"), ("app", "qa")])
def test_load_config_success(config_type, env):
    mock_yaml_content = {
        "dev": {"key": "value_dev"},
        "qa": {"key": "value_qa"}
    }
    with patch("builtins.open", mock_open(read_data="key: value")) as mock_file:
        with patch("yaml.safe_load", return_value=mock_yaml_content[env]):
            result = load_config(config_type, env)
            assert result == {"key": f"value_{env}"}
            mock_file.assert_called_once_with(f"../config/{config_type}_config.yaml", "r")

# Test file not found error
def test_load_config_file_not_found():
    with patch("builtins.open", side_effect=FileNotFoundError()):
        with pytest.raises(FileNotFoundError):
            load_config("app", "dev")

# Test YAML parsing error
def test_load_config_yaml_error():
    with patch("builtins.open", mock_open(read_data="key: value")):
        with patch("yaml.safe_load", side_effect=yaml.YAMLError):
            with pytest.raises(yaml.YAMLError):
                load_config("app", "dev")
