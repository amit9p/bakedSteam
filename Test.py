

import yaml
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def load_config(env: str, config_path: str = "config/app_config.yaml"):
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        return None
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        return None

    try:
        env_config = config.get('chamber', {}).get(env, {})
        stream_config = config.get('stream', {})
        onelake_dataset_config = config.get('onelake_dataset', {})
    except Exception as e:
        logger.error(f"Error accessing configuration for environment '{env}': {e}")
        return None

    return {
        'env_config': env_config,
        'stream_config': stream_config,
        'onelake_dataset_config': onelake_dataset_config.get(env, {}),
    }
