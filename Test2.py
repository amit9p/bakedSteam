

import boto3

def list_s3_objects_with_session(
    access_key: str,
    secret_key: str,
    session_token: str,
    region: str,
    bucket_name: str,
    prefix: str
):
    """
    Lists all objects under a specified S3 bucket folder,
    using temporary credentials (session token).
    """

    # Create the S3 client with the session token
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,  # include session token
        region_name=region
    )

    # Use a paginator to handle large listings automatically
    paginator = s3_client.get_paginator("list_objects_v2")

    # Paginate through all objects that start with the prefix
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        contents = page.get("Contents", [])
        for obj in contents:
            print(obj["Key"])

if __name__ == "__main__":
    list_s3_objects_with_session(
        access_key="<ACCESS_KEY>",
        secret_key="<SECRET_KEY>",
        session_token="<SESSION_TOKEN>",
        region="<REGION>",         # e.g., "us-east-1"
        bucket_name="<BUCKET_NAME>",
        prefix="myfolder/"         # or "" for entire bucket
    )
