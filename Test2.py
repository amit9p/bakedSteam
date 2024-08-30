
database:
  dev:
    host: localhost
    port: 5432
    username: dev_user
    password: dev_password
  qa:
    host: qa.db.server
    port: 5432
    username: qa_user
    password: qa_password
  prod:
    host: prod.db.server
    port: 5432
    username: prod_user
    password: prod_password

api:
  dev:
    base_url: http://localhost:5000/api
    timeout: 30
  qa:
    base_url: http://qa.api.server/api
    timeout: 60
  prod:
    base_url: https://api.server.com/api
    timeout: 120

logging:
  dev:
    level: DEBUG
    filepath: /var/log/dev_app.log
  qa:
    level: INFO
    filepath: /var/log/qa_app.log
  prod:
    level: WARNING
    filepath: /var/log/prod_app.log



import yaml

def load_config(env):
    """
    Load the configuration file and return the configuration for the specified environment.
    
    :param env: The environment to fetch the config for ('dev', 'qa', 'prod').
    :return: A dictionary containing the configuration for the specified environment.
    """
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
    
    # Create a dictionary to store environment-specific config
    env_config = {}
    
    # Loop through each section and fetch config for the specified environment
    for section, environments in config.items():
        if env in environments:
            env_config[section] = environments[env]
        else:
            raise ValueError(f"Environment '{env}' not found in section '{section}'")
    
    return env_config

# Example usage
if __name__ == "__main__":
    environment = "dev"  # Change this to 'qa' or 'prod' as needed
    config = load_config(environment)
    print(config)

    # Access specific configuration values
    db_host = config['database']['host']
    api_url = config['api']['base_url']
    log_level = config['logging']['level']

    print(f"Database Host: {db_host}")
    print(f"API URL: {api_url}")
    print(f"Log Level: {log_level}")

