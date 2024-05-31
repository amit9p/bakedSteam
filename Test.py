

import os
import unittest
from unittest.mock import patch, MagicMock

# Assuming load_config is the function you are testing
from your_module import load_config  # Replace 'your_module' with the actual module name

class TestLoadConfig(unittest.TestCase):

    @patch('your_module.os.path.join', return_value='config/app_config.yaml')
    @patch('your_module.open', side_effect=FileNotFoundError)
    def test_load_config_file_not_found(self, mock_open, mock_path_join):
        mock_logger = MagicMock()
        
        config = load_config('dev')
        
        self.assertIsNone(config)
        mock_logger.assert_called_once_with('Configuration file not found: config/app_config.yaml')

    @patch('your_module.os.path.join', return_value='config/app_config.yaml')
    @patch('your_module.open', new_callable=unittest.mock.mock_open, read_data='invalid_yaml_content')
    def test_load_config_yaml_error(self, mock_open, mock_path_join):
        mock_logger = MagicMock()
        
        config = load_config('dev')
        
        self.assertIsNone(config)
        mock_logger.assert_called_once()
        self.assertIn('Error parsing YAML file', mock_logger.call_args[0][0])

if __name__ == '__main__':
    unittest.main()
