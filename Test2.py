
def setup_turing_config(dev_cred, tokenization, env):
    env1 = "QA" if env in ["dev", "qa"] else "PROD"

    npi_turing_config = {
        "TURING_API_GATEWAY_NPI_URL": "https://api-turing-precede.cloud.capitalone.com" if env1 == "QA" else "https://api-turing-prcode.cloud.capitalone.com",
        "TURING_API_OAUTH_URL": "https://api-precode.cloud.capitalone.com" if env1 == "QA" else "https://api.cloud.capitalone.com",
        "TURING_API_NPI_SCOPE": f"tokenize:{tokenization}",
        "TURING_OAUTH_CLIENT_ID": dev_cred["client_id"],
        "TURING_OAUTH_CLIENT_SECRET": dev_cred["client_secret"],
        "TURING_CLIENT_SSL_VERIFY": False,
    }

    return npi_turing_config
