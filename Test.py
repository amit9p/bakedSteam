

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
