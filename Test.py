
def load_config(env):
    import yaml
    import os

    base_path = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_path, "/config/app_config.yaml")

    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            print(f"Config after yaml load: {config}")  # Diagnostic print
    except FileNotFoundError:
        print(f"Configuration file not found: {config_path}")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")
        return None

    # Assuming there's a dictionary access pattern that needs to be followed here
    try:
        env_config = config.get('env_config')
        stream_config = config.get('stream_config')
        oneLake_dataset_config = config.get('oneLake_dataset_config')
        print(f"Environment Config: {env_config}, Stream Config: {stream_config}, Dataset Config: {oneLake_dataset_config}")  # Diagnostic print
    except Exception as e:
        print(f"Error accessing configuration for environment {env}: {e}")
        return None

    return {
        'env_config': env_config,
        'stream_config': stream_config,
        'oneLake_dataset_config': oneLake_dataset_config
    }
