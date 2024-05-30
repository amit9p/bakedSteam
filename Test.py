

# utils/config_reader.py
import yaml

def load_config(env: str, config_path: str = "config/app_config.yaml"):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config.get('chamber', {}).get(env, {})


# ecbr_assembler/assembler/core.py
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
        logger.info(f"Loaded config for environment {env}: {config}")
        # Use config in your method calls
        # For example:
        # self.read_whole_parquet_file(config['some_key'])

        # Your other method calls using the config
        pass
