
^[A-Z0-9\s\-]{3,10}$




import re

# Regex from your JSON (double slashes escaped properly)
zip_regex = r"^\d{5}(-\d{4})?$"

# Compile regex
pattern = re.compile(zip_regex)

# Sample data
valid_zips = ["12345", "12345-6789"]
invalid_zips = ["1234", "123456", "1234-567", "12345-678", "abcd5", "12345-67890"]

# Validation function
def is_valid_zip(zip_code):
    return bool(pattern.match(zip_code))

# Test and print results
print("Valid ZIP Codes:")
for z in valid_zips:
    print(f"{z}: {is_valid_zip(z)}")

print("\nInvalid ZIP Codes:")
for z in invalid_zips:
    print(f"{z}: {is_valid_zip(z)}")








pattern = r"^\d{4}-\d{2}-\d{2}$"



import os

# Get absolute path to project root (assuming this runs from inside project)
project_root = os.path.dirname(os.path.abspath(__file__))
ivy_path = os.path.join(project_root, "..", "ivysettings.xml")  # adjust as needed

# Resolve to absolute path
ivy_path = os.path.abspath(ivy_path)

# Use it in your Spark config
spark = SparkSession.builder \
    .config("spark.jars.ivySettings", ivy_path) \
    ...







import subprocess
import argparse

def main():
    parser = argparse.ArgumentParser()

    # Common arguments you expect
    parser.add_argument("--env", required=True)
    parser.add_argument("--region", required=True)

    # Arguments specific to each script
    parser.add_argument("--bucket_one", required=True)
    parser.add_argument("--prefix_one", required=True)

    parser.add_argument("--bucket_two", required=True)
    parser.add_argument("--prefix_two", required=True)

    args = parser.parse_args()

    # Build command for read_s3_one.py
    read_s3_one_cmd = [
        "python", "read_s3_one.py",
        "--env", args.env,
        "--region", args.region,
        "--bucket", args.bucket_one,
        "--prefix", args.prefix_one
    ]

    # Build command for read_s3_two.py
    read_s3_two_cmd = [
        "python", "read_s3_two.py",
        "--env", args.env,
        "--region", args.region,
        "--bucket", args.bucket_two,
        "--prefix", args.prefix_two
    ]

    # Execute first script
    subprocess.check_call(read_s3_one_cmd)

    # Execute second script
    subprocess.check_call(read_s3_two_cmd)

if __name__ == "__main__":
    main()

python main_subprocess_runner.py \
  --env dev \
  --region us-east-1 \
  --bucket_one my-first-bucket \
  --prefix_one folder1/ \
  --bucket_two my-second-bucket \
  --prefix_two folder2/



import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    args = parser.parse_args()

    print("Running read_s3_one with:")
    print(f"ENV: {args.env}, REGION: {args.region}, BUCKET: {args.bucket}, PREFIX: {args.prefix}")

if __name__ == "__main__":
    main()







