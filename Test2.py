
import yaml
import requests
import os

def load_secrets():
    """Loads client credentials from secrets.yaml."""
    secrets_path = os.path.join("config", "secrets.yaml")
    with open(secrets_path, "r") as f:
        secrets = yaml.safe_load(f)
    return secrets["auth"]

def get_token():
    """Fetches OAuth token using client credentials."""
    creds = load_secrets()
    client_id = creds["client_id"]
    client_secret = creds["client_secret"]
    token_url = creds["token_url"]

    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(token_url, data=payload, headers=headers)
    response.raise_for_status()

    return response.json()["access_token"]


auth:
  client_id: "your-client-id-here"
  client_secret: "your-client-secret-here"
  token_url: "https://api.capitalone.com/oauth/token"

{
  "s3_path": "s3://your-bucket/input/",
  "file_list": [
    "file1.csv",
    "file2.csv",
    "file3.csv",
    "file4.csv",
    "file5.csv",
    "file6.csv",
    "file7.csv",
    "file8.csv"
  ]
}



import json
import os
from datetime import datetime

# Import your custom modules
from publisher.onelake_upload import publish_file
from validator.file_validator import validate_submission
from report_generator.report_gen import generate_report  # optional if you create one

CONFIG_PATH = os.path.join("config", "config.json")


def load_config(config_path):
    """Reads the config file to get S3 path and file list."""
    with open(config_path, "r") as f:
        config = json.load(f)
    return config["s3_path"], config["file_list"]


def main():
    s3_path, file_list = load_config(CONFIG_PATH)

    results = []

    for file_name in file_list:
        print(f"📤 Publishing {file_name} ...")

        # Step 1: Publish API Call
        try:
            response = publish_file(s3_path, file_name)
            file_submission_id = response.get("fileSubmissionId")

            if not file_submission_id:
                raise ValueError("No fileSubmissionId returned in API response")

            print(f"✅ Published {file_name}, FileSubmissionID: {file_submission_id}")

            # Step 2: Validate the submission
            validation_status = validate_submission(file_submission_id)
            print(f"🔍 Validation for {file_name}: {validation_status}")

            results.append({
                "file_name": file_name,
                "file_submission_id": file_submission_id,
                "validation_status": validation_status,
                "timestamp": datetime.utcnow().isoformat()
            })

        except Exception as e:
            print(f"❌ Failed processing {file_name}: {str(e)}")
            results.append({
                "file_name": file_name,
                "file_submission_id": None,
                "validation_status": "FAILED",
                "error": str(e)
            })

    # Step 3: Generate final report
    report_file = "publish_report.json"
    with open(report_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n📄 Report generated: {report_file}")

    # Optional: pretty console summary
    for r in results:
        print(f"{r['file_name']} → {r['validation_status']} (ID: {r.get('file_submission_id')})")


if __name__ == "__main__":
    main()



import requests

def publish_file(s3_path, file_name):
    """Publishes a file and returns API JSON response."""
    url = "https://api.capitalone.com/publish"  # example placeholder
    payload = {"s3Path": f"{s3_path}{file_name}"}
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()



import requests

def validate_submission(file_submission_id):
    """Checks if file submission succeeded."""
    url = f"https://api.capitalone.com/validate/{file_submission_id}"
    response = requests.get(url)
    response.raise_for_status()
    result = response.json()
    return result.get("status", "UNKNOWN")



