
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def get_aws_session(aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, region_name=None):
    """
    Create an AWS session using provided credentials or environment variables.
    """
    try:
        if aws_access_key_id and aws_secret_access_key:
            # Create a session using provided credentials
            session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=region_name
            )
        else:
            # Use default credentials from environment variables or ~/.aws/credentials
            session = boto3.Session(region_name=region_name)
        
        return session
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Credentials not available: ", e)
        return None

def list_s3_files(session, bucket_name, prefix):
    """
    List all file names in a specific S3 bucket and prefix.
    """
    try:
        s3 = session.client('s3')
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' in response:
            files = [item['Key'] for item in response['Contents']]
            return files
        else:
            print("No files found in the specified S3 bucket and prefix.")
            return []
    except Exception as e:
        print("Error occurred while listing files: ", e)
        return []

def main():
    # AWS credentials (optional, use None to rely on environment variables or ~/.aws/credentials)
    aws_access_key_id = None  # Replace with your access key or keep None
    aws_secret_access_key = None  # Replace with your secret key or keep None
    aws_session_token = None  # Replace with your session token or keep None
    region_name = 'us-east-1'  # Replace with your region or keep None for default

    # S3 bucket and prefix
    bucket_name = 'your-s3-bucket-name'
    prefix = 'your/folder/prefix/'  # The folder or prefix in the S3 bucket

    # Create an AWS session
    session = get_aws_session(aws_access_key_id, aws_secret_access_key, aws_session_token, region_name)
    
    if session:
        # List files in the S3 bucket and prefix
        files = list_s3_files(session, bucket_name, prefix)
        
        # Print the file names
        for file_name in files:
            print(file_name)

if __name__ == "__main__":
    main()
