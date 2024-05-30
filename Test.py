

# utils/config_reader.py
import yaml

def load_config(env: str, config_path: str = "config/app_config.yaml"):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config.get('chamber', {}).get(env, {})
