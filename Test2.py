

import yaml
from pathlib import Path

# -------------------------------------------------------------------
# 📝 USER INPUT SECTION
# Provide the dataset number below (choose 1–11)
# 1 → account_service_account_os
# 2 → credit_bureau_reporting_card_cl
# 3 → loan_application_ls
# etc.
# -------------------------------------------------------------------

dataset_number = 1  # 👈 user changes this number only

# -------------------------------------------------------------------
# Load dataset config
# -------------------------------------------------------------------
def load_dataset_config(dataset_number: int):
    config_path = Path(__file__).resolve().parent / "config.yaml"

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    datasets = config.get("datasets", {})
    dataset_info = datasets.get(dataset_number)

    if not dataset_info:
        raise ValueError(f"❌ Invalid dataset number: {dataset_number}")

    print(f"✅ Selected Dataset: {dataset_info['name']}")
    print(f"📦 S3 Path: {dataset_info['s3_path']}")
    print(f"🪣 Catalog ID: {dataset_info['catalog_id']}")
    return dataset_info


if __name__ == "__main__":
    dataset = load_dataset_config(dataset_number)

    # Example of how you can use it:
    s3_path = dataset["s3_path"]
    catalog_id = dataset["catalog_id"]
    name = dataset["name"]

    # Here you can call your list_objects or spark job
    print(f"Now listing objects under: {s3_path}")
    # get_ol_object_list(sparkContext, s3_path, hconf)







-----

datasets:
  1:
    name: account_service_account_os
    s3_path: s3://your-bucket/path/to/account_service_account_os/
    catalog_id: 7d2981d5-4a5a-49f7-a937-8f18f5ad0a44

  2:
    name: credit_bureau_reporting_card_cl
    s3_path: s3://your-bucket/path/to/credit_bureau_reporting_card_cl/
    catalog_id: 45d27c2e-3b44-4b08-9a2d-b9500a092d68

  3:
    name: loan_application_ls
    s3_path: s3://your-bucket/path/to/loan_application_ls/
    catalog_id: 7327626d-8d5c-450a-9c84-14250d00b29d

  # … add all 11 datasets here
