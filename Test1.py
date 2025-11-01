

import os
from botocore.exceptions import ClientError, BotoCoreError

def upload_csv_directory(local_dir, bucket, prefix, s3_client):
    """
    Uploads all .csv files from a local directory to S3.
    
    Args:
        local_dir (str): Path to local directory containing CSV files.
        bucket (str): S3 bucket name (without s3://).
        prefix (str): Folder path inside bucket (e.g., 'incoming/data/').
        s3_client: boto3 S3 client object.
    """
    uploaded = 0

    for file_name in os.listdir(local_dir):
        if file_name.lower().endswith(".csv"):
            file_path = os.path.join(local_dir, file_name)
            key = f"{prefix}{file_name}" if prefix else file_name
            try:
                s3_client.upload_file(file_path, bucket, key)
                print(f"✅ Uploaded {file_name} → s3://{bucket}/{key}")
                uploaded += 1
            except (ClientError, BotoCoreError) as e:
                print(f"❌ Failed to upload {file_name}: {e}")

    if uploaded == 0:
        print("⚠️ No CSV files found in the specified directory.")
    else:
        print(f"✅ Successfully uploaded {uploaded} CSV files.")


if __name__ == "__main__":
    local_directory = "/Users/vmq434/PycharmProjects/cos/PythonProject/input_csvs"
    bucket_name = "ecbr-coaf-al-qa-ol-file-puller-data-fuel"
    prefix = "dfs/dfs_datasets/"   # folder in your bucket

    upload_csv_directory(local_directory, bucket_name, prefix, s3_client)
