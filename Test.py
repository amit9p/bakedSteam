

def read_config(self):
    print("Reading configuration from YAML file...")
    try:
        with open(self.yaml_file, 'r') as file:
            configfile = yaml.load(file, Loader=yaml.SafeLoader)
            print("Configuration loaded successfully.")
    except FileNotFoundError:
        print("Configuration file not found.")
        return None
    except yaml.YAMLError as exc:
        print("Error parsing configuration file:", exc)
        return None
    return configfile


import yaml

class Configs:
    def __init__(self):
        self.yaml_file = '/path/to/your/app_config.yaml'  # Update the path as necessary

    def read_config(self):
        print("Reading configuration from YAML file...")
        with open(self.yaml_file, 'r') as file:
            configfile = yaml.load(file, Loader=yaml.SafeLoader)
            print("Configuration loaded successfully.")
        return configfile

    def test(self):
        print("hello world")
