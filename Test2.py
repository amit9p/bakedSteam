
import logging
import yaml
import os

# Initialize the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass

def load_config(config_type, env):
    base_path = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_path, "../config/app_config.yaml")

    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
            logger.info(f"Config after yaml load: {config}")
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise ConfigError(f"Configuration file not found: {config_path}")
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise ConfigError(f"Error parsing YAML file: {e}")

    # Assuming there's a dictionary access pattern that needs to be followed here
    try:
        env_config = config.get(config_type, {}).get(env, {})
        logger.info(f"Environment Config: {env_config}")
    except Exception as e:
        logger.error(f"Error accessing configuration for environment {env}: {e}")
        raise ConfigError(f"Error accessing configuration for environment {env}: {e}")

    return {"env_config": env_config}




###
import unittest
from unittest.mock import patch, mock_open
from config_reader import load_config, ConfigError

class TestConfigReader(unittest.TestCase):

    @patch("builtins.open", side_effect=FileNotFoundError)
    @patch("os.path.join", return_value="/config/app_config.yaml")
    @patch("os.path.dirname", return_value="/")
    def test_load_config_file_not_found(self, mock_dirname, mock_path_join, mock_file):
        with self.assertRaises(ConfigError) as context:
            load_config(config_type="chamber", env="dev")
        self.assertIn("Configuration file not found", str(context.exception))

    @patch("builtins.open", new_callable=mock_open, read_data="not a yaml file")
    @patch("os.path.join", return_value="/config/app_config.yaml")
    @patch("os.path.dirname", return_value="/")
    def test_load_config_yaml_error(self, mock_dirname, mock_path_join, mock_file):
        with self.assertRaises(ConfigError) as context:
            load_config(config_type="chamber", env="dev")
        self.assertIn("Error parsing YAML file", str(context.exception))

    # You can add more tests here as needed

if __name__ == "__main__":
    unittest.main()

