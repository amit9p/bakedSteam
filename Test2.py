
from utils.config_reader import load_config
from ecbr_logging import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def get_cli_creds(credential_type, env):
    try:
        chamber_config = load_config(credential_type, env)
        if chamber_config is None:
            raise ValueError(f"Configuration could not be loaded for environment: {env}")

        chamber_env_config = chamber_config.get('env_config')
        if chamber_env_config is None:
            raise ValueError(f"Environment config missing for environment: {env}")

        logger.info(f"Loaded environment config for {env}: {chamber_env_config}")

        domain = chamber_env_config.get('CHAMBER_URL')
        if domain is None:
            raise ValueError("CHAMBER_URL is missing in environment config")

        role = chamber_env_config.get('VAULT_ROLE')
        if role is None:
            raise ValueError("VAULT_ROLE is missing in environment config")

        lockbox_id = chamber_env_config.get('LOCKBOX_ID')
        if lockbox_id is None:
            raise ValueError("LOCKBOX_ID is missing in environment config")

        client_id_path = chamber_env_config.get('CLIENT_ID_PATH')
        if client_id_path is None:
            raise ValueError("CLIENT_ID_PATH is missing in environment config")

        client_secret_path = chamber_env_config.get('CLIENT_SECRET_PATH')
        if client_secret_path is None:
            raise ValueError("CLIENT_SECRET_PATH is missing in environment config")

        env1 = 'qa' if env in ['dev', 'qa'] else 'PROD'

        if env1 == 'qa':
            dev_creds = {
                "client_id": "CLIENTID",
                "client_secret": "CLIENTSECRET"
            }
        else:
            c = IamClient(domain=domain, role=role, lockbox_id=lockbox_id)
            dev_creds = {
                "client_id": c.get_secret_from_path(path=client_id_path, secret_key="client_id"),
                "client_secret": c.get_secret_from_path(path=client_secret_path, secret_key="client_secret")
            }

            if dev_creds['client_id'] is None:
                raise ValueError("client_id could not be retrieved from the secret path")
            
            if dev_creds['client_secret'] is None:
                raise ValueError("client_secret could not be retrieved from the secret path")

        return dev_creds
    except ValueError as e:
        logger.error(f"ValueError: {e}")
        raise
