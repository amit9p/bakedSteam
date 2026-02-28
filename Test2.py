
def create_spark_session(aws_creds: Dict[str, str]):

    spark = (
        SparkSession.builder
        .appName("PySpark AWS S3 Example")

        # ✅ Maven packages (auto-download)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )

        # ✅ Ivy cache (optional but recommended)
        .config(
            "spark.jars.ivy",
            "/tmp/.ivy2"
        )

        # ✅ S3A filesystem
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # ✅ Credentials
        .config("spark.hadoop.fs.s3a.access.key", aws_creds["aws_access_key_id"])
        .config("spark.hadoop.fs.s3a.secret.key", aws_creds["aws_secret_access_key"])
        .config("spark.hadoop.fs.s3a.session.token", aws_creds["aws_session_token"])

        # ✅ Endpoint & access style
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", True)

        # Local dev stability
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")

        .getOrCreate()
    )

    return spark




from typing import Dict

def read_aws_credentials(file_path: str) -> Dict[str, str]:
    """
    Reads an AWS credentials file and returns required key-value pairs.

    :param file_path: Path to credentials file
    :return: Dictionary with AWS credentials
    """

    required_keys = {
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "aws_security_token",
    }

    credentials: Dict[str, str] = {}

    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()

            # Skip empty lines or invalid lines
            if not line or "=" not in line:
                continue

            key, value = line.split("=", 1)

            key = key.strip()
            value = value.strip()

            if key in required_keys:
                credentials[key] = value

    return credentials


creds = read_aws_credentials("/path/to/credentials")

print(creds)


aws_creds = read_aws_credentials("/path/to/credentials")

spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.access.key", aws_creds["aws_access_key_id"])
    .config("spark.hadoop.fs.s3a.secret.key", aws_creds["aws_secret_access_key"])
    .config("spark.hadoop.fs.s3a.session.token", aws_creds["aws_session_token"])
    .getOrCreate()
)
