
# s3_ops.py
import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# 1) Choose the profile that CloudSentry populated (or leave None for "default")
AWS_PROFILE = os.getenv("AWS_PROFILE", "default")   # e.g., "GR_GG_COF_AWS_STsdigital_Dev_Developer"
AWS_REGION  = os.getenv("AWS_REGION",  "us-east-1") # set if your bucket is region-specific

# 2) Create a session that reads ~/.aws/credentials and ~/.aws/config
session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
s3 = session.client("s3")

def list_objects(bucket: str, prefix: str = ""):
    """Print all object keys under a prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            print(obj["Key"])

def upload_file(local_path: str, bucket: str, key: str):
    """Upload a local file to S3 (uses multipart automatically)."""
    try:
        s3.upload_file(local_path, bucket, key)  # best for files
        print(f"✅ Uploaded {local_path} -> s3://{bucket}/{key}")
    except (ClientError, BotoCoreError) as e:
        print(f"❌ Upload failed: {e}")

def put_text(bucket: str, key: str, text: str):
    """Create/overwrite a small text object."""
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))
        print(f"✅ Put text to s3://{bucket}/{key}")
    except (ClientError, BotoCoreError) as e:
        print(f"❌ Put failed: {e}")

if __name__ == "__main__":
    # EXAMPLES
    BUCKET = "your-bucket"
    PREFIX = "incoming/"
    list_objects(BUCKET, PREFIX)

    upload_file("/path/to/local.csv", BUCKET, f"{PREFIX}local.csv")
    put_text(BUCKET, f"{PREFIX}hello.txt", "hello from local!")
