

import pytest
from unittest.mock import patch, mock_open, MagicMock
from config_reader import config_read

@pytest.fixture
def mock_yaml_data():
    return {
        "chamber": {"qa": "QA config data"},
        "stream": {"qa": "QA stream data"},
        "oneLake_dataset": {"qa": "QA dataset data"}
    }

def test_load_config_success(mock_yaml_data):
    # Mock the open function to return the YAML data
    m = mock_open(read_data="yaml data here")
    with patch("builtins.open", m):
        with patch("os.path.join", return_value="/fake/path/app_config.yaml"):
            with patch("os.path.abspath", return_value="/fake/path"):
                # Mock yaml.safe_load to return the mock_yaml_data
                with patch("yaml.safe_load", return_value=mock_yaml_data):
                    # Mock the get methods to return appropriate mock data
                    with patch.object(config_read, 'get', MagicMock(side_effect=lambda x: mock_yaml_data[x])):
                        config = config_read.load_config("qa")
                        assert config is not None
                        assert config['env_config'] == "QA config data"
                        assert config['stream_config'] == "QA stream data"
                        assert config['oneLake_dataset_config'] == "QA dataset data"

# Ensure this test function name matches with your pytest framework settings if it has specific rules.
