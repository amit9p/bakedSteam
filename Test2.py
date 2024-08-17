
import configparser

def get_aws_credentials(account_name):
    # Define the path to the AWS credentials file
    credentials_file = '~/.aws/credentials'
    
    # Expand the user path
    credentials_file = os.path.expanduser(credentials_file)
    
    # Create a ConfigParser object
    config = configparser.ConfigParser()
    
    # Read the AWS credentials file
    config.read(credentials_file)
    
    # Check if the account exists in the credentials file
    if account_name in config:
        aws_access_key_id = config[account_name]['aws_access_key_id']
        aws_secret_access_key = config[account_name]['aws_secret_access_key']
        aws_session_token = config[account_name]['aws_session_token']
        
        return {
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key,
            'aws_session_token': aws_session_token
        }
    else:
        raise Exception(f"Account {account_name} not found in the AWS credentials file.")

# Account name to fetch the credentials for
account_name = "GR_GG_COF_AWS_592502317603_Developer"

# Fetch the credentials
try:
    credentials = get_aws_credentials(account_name)
    print(f"AWS Access Key ID: {credentials['aws_access_key_id']}")
    print(f"AWS Secret Access Key: {credentials['aws_secret_access_key']}")
    print(f"AWS Session Token: {credentials['aws_session_token']}")
except Exception as e:
    print(e)
