
def get_config_value(config, key):
    value = config.get(key)
    if value is None:
        raise ValueError(f"Configuration key '{key}' not found.")
    return value



import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def get_cli_creds(credential_type, env):
    try:
        chamber_config = load_config(credential_type, env)
        if chamber_config is None:
            raise ValueError(f"Configuration could not be loaded for environment: {env}")

        chamber_env_config = get_config_value(chamber_config, "env_config")
        logger.info(f"Loaded environment config for {env}: {chamber_env_config}")

        domain = get_config_value(chamber_env_config, "CHAMBER_URL")
        role = get_config_value(chamber_env_config, "VAULT_ROLE")
        lockbox_id = get_config_value(chamber_env_config, "LOCKBOX_ID")
        client_id_path = get_config_value(chamber_env_config, "CLIENT_ID_PATH")
        client_secret_path = get_config_value(chamber_env_config, "CLIENT_SECRET_PATH")

        env1 = "qa" if env in ["dev", "qa", "local"] else "prod"

        if env1 == "qa":
            dev_creds = {
                "client_id": CLIENTID,
                "client_secret": CLIENTSECRET
            }
        else:
            iam_client = IamClient(domain=domain, role=role, lockbox_id=lockbox_id)
            dev_creds = {
                "client_id": iam_client.get_secret_from_path(path=client_id_path, secret_key="client_id"),
                "client_secret": iam_client.get_secret_from_path(path=client_secret_path, secret_key="client_secret")
            }

        return dev_creds

    except ValueError as e:
        logger.error(f"ValueError: {e}")
        raise
