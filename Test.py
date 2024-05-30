
from pyspark.sql import SparkSession
from ecbr.logging import logging
from utils.config_reader import load_config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Assembler:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def read_whole_parquet_file(self, path):
        try:
            df = self.spark.read.parquet(path)
            return df
        except Exception as e:
            logger.error(f"Error reading parquet file at {path}: {e}")
            return None

    def run(self, env: str, dataset_id: str, business_dt: str, run_id: str, file_type: str = "ALL"):
        try:
            config = load_config(env)
            if config is None:
                raise ValueError(f"Configuration could not be loaded for environment: {env}")
            
            env_config = config['env_config']
            stream_config = config['stream_config']
            onelake_dataset_config = config['onelake_dataset_config']
            
            logger.info(f"Loaded environment config for {env}: {env_config}")
            logger.info(f"Loaded stream config: {stream_config}")
            logger.info(f"Loaded OneLake dataset config for {env}: {onelake_dataset_config}")

            # Example of accessing environment-specific configurations
            vault_role = env_config.get('VAULT_ROLE')
            logger.info(f"Vault Role for {env}: {vault_role}")
            
            # Example of accessing stream-specific configurations
            daily_accounts_config = stream_config.get('daily_accounts', {}).get(env)
            logger.info(f"Daily Accounts Config for {env}: {daily_accounts_config}")

            # Example of accessing OneLake dataset configurations
            onelake_daily_accounts = onelake_dataset_config.get('daily_accounts')
            logger.info(f"OneLake Daily Accounts for {env}: {onelake_daily_accounts}")

            # Use config in your method calls
            # For example:
            # self.read_whole_parquet_file(env_config['some_key'])

            # Your other method calls using the config
            pass
        except ValueError as e:
            logger.error(e)
        except Exception as e:
            logger.error(f"An error occurred during the run: {e}")
