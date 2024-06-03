
from pyspark.sql import SparkSession
import os
import subprocess

# Get the JAVA_HOME path using the system command
java_home = subprocess.check_output(['/usr/libexec/java_home']).strip().decode('utf-8')

# Set the environment variable in the current Python process
os.environ['JAVA_HOME'] = java_home

# Verify the environment variable
print('JAVA_HOME:', os.environ['JAVA_HOME'])

# Set the AWS_PROFILE environment variable
os.environ['AWS_PROFILE'] = 'GR_GG_COF_AWS_592502317603_Developer'

# Verify that the environment variable is set by printing it
print('AWS_PROFILE:', os.environ['AWS_PROFILE'])

# Define your AWS credentials
aws_access_key_id = 'your_access_key_id'
aws_secret_access_key = 'your_secret_access_key'

# Path to the downloaded JAR files
hadoop_aws_jar = "/path/to/hadoop-aws-3.3.4.jar"
aws_java_sdk_jar = "/path/to/aws-java-sdk-bundle-1.12.524.jar"

# Create a Spark session with the local JAR files
spark = SparkSession.builder \
    .appName("PySpark AWS S3 Example") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.jars", f"{hadoop_aws_jar},{aws_java_sdk_jar}") \
    .getOrCreate()

# Example: Read a Parquet file from S3
s3_file_path = "s3a://ecbr-coaf-al-dev-e1-file-puller-data-fuel/assembler_input/ECBR_TU_TKNZD_parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(s3_file_path)

# Show the DataFrame
df.show()
