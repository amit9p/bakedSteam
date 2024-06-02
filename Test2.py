

import boto3
import pandas as pd
import io
from botocore.config import Config

# Define AWS credentials and bucket/file information
aws_access_key_id = 'your_access_key_id'
aws_secret_access_key = 'your_secret_access_key'
aws_session_token = 'your_session_token'  # Optional, only if you are using temporary credentials
bucket_name = 'your_bucket_name'
file_key = 'path/to/your/file.json'

# Initialize a session using your credentials
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token  # Optional
)

# Config to skip SSL verification
config = Config(
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    },
    ssl_verify=False
)

# Use the session to create an S3 client with the specified config
s3_client = session.client('s3', config=config)

# Get the JSON file from S3
obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
data = obj['Body'].read()

# Read the JSON file into a Pandas DataFrame
df = pd.read_json(io.BytesIO(data))

# Display the DataFrame
print(df)
