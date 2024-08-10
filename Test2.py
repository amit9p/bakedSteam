
import boto3

# Initialize a session using the default or a specific profile
session = boto3.Session(profile_name='your-profile-name')  # Use profile_name=None for the default profile

# Retrieve the credentials
credentials = session.get_credentials()

# Access the credentials
aws_access_key_id = credentials.access_key
aws_secret_access_key = credentials.secret_key
aws_session_token = credentials.token

print(f"AWS Access Key: {aws_access_key_id}")
print(f"AWS Secret Key: {aws_secret_access_key}")
print(f"Session Token: {aws_session_token}")
