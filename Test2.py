
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







