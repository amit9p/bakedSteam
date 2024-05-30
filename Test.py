

import yaml

def load_config(env: str, config_path: str = "config/app_config.yaml"):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Extract the environment-specific configuration
    env_config = config.get('chamber', {}).get(env, {})
    
    # Extract other top-level configurations that might be needed
    stream_config = config.get('stream', {})
    
    # Return a combined dictionary with both environment and other relevant configurations
    return {
        'env_config': env_config,
        'stream_config': stream_config,
    }
