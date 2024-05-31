
def main():
    print("hey there")
    spark = SparkSession.builder.appName("test_app").getOrCreate()
    assembler = Assembler(spark)
    assembler.run(
        env="dev",
        dataset_id="fdfef7232twelvefdhytvdy",
        business_dt="2024-05-30",
        run_id="123243535353353",
        file_type="TU"
    )

if __name__ == "__main__":
    main()






class Assembler:
    def run(self, **kwargs):
        env = kwargs.get('env')
        dataset_id = kwargs.get('dataset_id')
        business_dt = kwargs.get('business_dt')
        run_id = kwargs.get('run_id')
        file_type = kwargs.get('file_type', 'ALL')

        try:
            print('1')
            config = load_config(env)
            if config is None:
                raise ValueError(f"Configuration could not be loaded for environment: {env}")

            env_config = config['env_config']
            stream_config = config['stream_config']
            onelake_dataset_config = config['onelake_dataset_config']

            logger.info(f"Loaded environment config for {env}: {env_config}")
            logger.info(f"Loaded stream config for {env}: {stream_config}")
            logger.info(f"Loaded OneLake dataset config for {env}: {onelake_dataset_config}")

            vault_role = env_config.get("VAULT_ROLE")
            logger.info(f"Vault Role for {env}: {vault_role}")

            daily_accounts_config = stream_config.get("daily_accounts", {}).get(env)
            logger.info(f"Daily Accounts Config for {env}: {daily_accounts_config}")

            onelake_daily_accounts = onelake_dataset_config.get("daily_accounts")
            logger.info(f"OneLake Daily Accounts for {env}: {onelake_daily_accounts}")

            # Method call to read parquet file
            # Method call to de tokeninze dataframes
            # Method call to generate metro2 string for each file type
            # Method call to write metro2 file on EFG s3

        except ValueError as e:
            logger.error(e)
        except Exception as e:
            logger.error(e)
