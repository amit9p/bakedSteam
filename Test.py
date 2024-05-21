

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

from pyspark.sql import functions as F

# Assuming 'df' is your DataFrame already loaded with data.

# Filter the DataFrame where 'account_id' is not null and 'tokenization_type' is 'ustaxid'
filtered_df = df.filter(
    (F.col("account_id").isNotNull()) & 
    (F.col("tokenization_type") == "ustaxid")
)

# Select the required columns
final_df = filtered_df.select(
    "run_id", "account_id", "attribute", "value", "file_type", "tokenization_type"
)

# Show the resulting DataFrame
final_df.show()


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
