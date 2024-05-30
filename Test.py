

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


from pyspark.sql import SparkSession
from ecbr.logging import logging
from utils.config_reader import load_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Assembler:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def read_whole_parquet_file(self, path):
        df = self.spark.read.parquet(path)
        return df

    def run(self, env: str, dataset_id: str, business_dt: str, run_id: str, file_type: str = "ALL"):
        config = load_config(env)
        
        env_config = config['env_config']
        stream_config = config['stream_config']
        
        logger.info(f"Loaded environment config for {env}: {env_config}")
        logger.info(f"Loaded stream config: {stream_config}")

        # Example of accessing environment-specific configurations
        vault_role = env_config.get('VAULT_ROLE')
        logger.info(f"Vault Role for {env}: {vault_role}")
        
        # Example of accessing stream-specific configurations
        daily_accounts_config = stream_config.get('daily_accounts', {}).get(env)
        logger.info(f"Daily Accounts Config for {env}: {daily_accounts_config}")

        # Use config in your method calls
        # For example:
        # self.read_whole_parquet_file(env_config['some_key'])

        # Your other method calls using the config
        pass
