
import subprocess
import shlex

def fetch_cloudsentry_credentials(account_id: str, ba_code: str):
    cmd = f"cloudsentry access get --account={account_id} --ba={ba_code}"
    # -l loads login config; -c runs the command
    out = subprocess.run(["/bin/zsh", "-lc", cmd], capture_output=True, text=True, check=True)
    return out.stdout

-------
import subprocess

def fetch_cloudsentry_credentials(account_id: str, ba_code: str):
    """
    Runs the 'cloudsentry access get' command and returns its output.
    """
    cmd = [
        "cloudsentry",
        "access",
        "get",
        f"--account={account_id}",
        f"--ba={ba_code}"
    ]

    try:
        # Run the command and capture the output
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print("✅ Cloudsentry credentials fetched successfully!")
        return result.stdout.strip()

    except subprocess.CalledProcessError as e:
        print("❌ Failed to fetch credentials:")
        print(e.stderr.strip())
        return None


# Example usage
if __name__ == "__main__":
    account_id = "5665566"
    ba_code = "BAECBR"

    output = fetch_cloudsentry_credentials(account_id, ba_code)
    if output:
        print("\nCommand Output:\n", output)

# s3_from_aws_credentials.py
import os
from pathlib import Path
from configparser import RawConfigParser
import boto3
from botocore.exceptions import BotoCoreError, ClientError

def load_creds_from_files(profile: str = "default"):
    """Read access keys (and optional session token) from ~/.aws/credentials"""
    creds_path = Path.home() / ".aws" / "credentials"
    cfg = RawConfigParser()
    if not cfg.read(creds_path):
        raise FileNotFoundError(f"Could not read {creds_path}")

    if not cfg.has_section(profile):
        raise KeyError(f"Profile [{profile}] not found in {creds_path}")

    return {
        "aws_access_key_id":     cfg.get(profile, "aws_access_key_id", fallback=None),
        "aws_secret_access_key": cfg.get(profile, "aws_secret_access_key", fallback=None),
        "aws_session_token":     cfg.get(profile, "aws_session_token", fallback=None),  # may be empty
    }

def load_region(profile: str = "default", fallback_region: str = "us-east-1"):
    """Read region from ~/.aws/config (handles [profile <name>] format)"""
    config_path = Path.home() / ".aws" / "config"
    cfg = RawConfigParser()
    cfg.read(config_path)

    section = "default" if profile == "default" else f"profile {profile}"
    return cfg.get(section, "region", fallback=fallback_region)

def make_s3_client(profile: str = "default", verify_ssl: bool | str | None = None):
    """
    verify_ssl:
      - False  → disable verification (not recommended, debug only)
      - None   → default system trust store
      - str    → path to a CA bundle (recommended in corporate networks)
    """
    creds = load_creds_from_files(profile)
    region = load_region(profile)

    return boto3.client(
        "s3",
        region_name=region,
        verify=verify_ssl,
        **{k: v for k, v in creds.items() if v}  # drop None
    )

def list_objects(bucket: str, prefix: str = "", profile: str = "default", verify_ssl: bool | str | None = None):
    s3 = make_s3_client(profile, verify_ssl)
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                print(obj["Key"])
    except (ClientError, BotoCoreError) as e:
        print("Error:", e)

if __name__ == "__main__":
    # ----- EXAMPLE USAGE -----
    PROFILE = os.getenv("AWS_PROFILE", "default")
    BUCKET  = "my-data-bucket"          # <- bucket NAME only (no s3://)
    PREFIX  = "incoming/data/"          # <- optional "folder" path; "" lists all

    # Option A (debug only): disable SSL verification
    list_objects(BUCKET, PREFIX, profile=PROFILE, verify_ssl=False)

    # Option B (recommended in corp networks): use your corporate CA bundle
    # list_objects(BUCKET, PREFIX, profile=PROFILE, verify_ssl="/path/to/corporate-ca.pem")
