
def setup_turing_config(dev_cred, env, tokenization):
    env1 = "QA" if env in ["dev", "qa"] else "PROD"

    if env1 == "QA":
        npi_turing_config = {
            "TURING_API_GATEWAY_URL": "https://api-turing-precede.cloud.capitalone.com",
            "TURING_API_OAUTH_URL": "https://api-precode.cloud.capitalone.com",
            "TURING_OAUTH_CLIENT_ID": dev_cred["client_id"],
            "TURING_OAUTH_CLIENT_SECRET": dev_cred["client_secret"],
            "TURING_CLIENT_SSL_VERIFY": False,
        }
    else:
        npi_turing_config = {
            "TURING_API_GATEWAY_URL": "https://api.cloud.capitalone.com",
            "TURING_API_OAUTH_URL": "https://api.cloud.capitalone.com",
            "TURING_OAUTH_CLIENT_ID": dev_cred["client_id"],
            "TURING_OAUTH_CLIENT_SECRET": dev_cred["client_secret"],
            "TURING_CLIENT_SSL_VERIFY": False,
        }

    # Set the specific scope key based on the tokenization value
    if tokenization == "ustaxid":
        npi_turing_config["TURING_API_NPI_SCOPE"] = f"tokenize:{tokenization}"
    elif tokenization == "pan":
        npi_turing_config["TURING_API_PCI_SCOPE"] = f"tokenize:{tokenization}"

    return npi_turing_config
