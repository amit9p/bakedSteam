

import boto3

# AWS Credentials (Replace with your values)
aws_access_key = "your-access-key"
aws_secret_key = "your-secret-key"
aws_session_token = "your-session-token"
bucket_name = "your-bucket-name"
prefix = ""  # Optional: Specify a prefix to list specific folders

# Initialize S3 client with SSL verification disabled
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    aws_session_token=aws_session_token,
    verify=False  # Disables SSL verification
)

# List objects in the S3 bucket
try:
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if "Contents" in response:
        print("Files in S3 Bucket:")
        for obj in response["Contents"]:
            print(obj["Key"])
    else:
        print("No files found in the specified bucket/prefix.")

except Exception as e:
    print(f"Error: {e}")
