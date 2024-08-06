

import configparser

def load_secrets(filename="secret.properties"):
    config = configparser.ConfigParser()
    config.read(filename)
    return config['DEFAULT']['client_id'], config['DEFAULT']['client_secret']


# Line 26 replacement
client_id, client_secret = load_secrets()
dev_creds = {
    "client_id": client_id,
    "client_secret": client_secret
}



[DEFAULT]
client_id=your_client_id_here
client_secret=your_client_secret_here
