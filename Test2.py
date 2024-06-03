
import os
import subprocess

# Set the AWS_PROFILE environment variable
os.environ['AWS_PROFILE'] = 'GR_GG_COF_AWS_592502317603_Developer'

# Verify that the environment variable is set by printing it
print('AWS_PROFILE:', os.environ['AWS_PROFILE'])

# If you want to run a command that uses this environment variable in a subprocess
command = 'echo $AWS_PROFILE'
result = subprocess.run(command, shell=True, env=os.environ, capture_output=True, text=True)
print('Subprocess AWS_PROFILE:', result.stdout.strip())


______

import os
import subprocess

# Get the JAVA_HOME path using the system command
java_home = subprocess.check_output(['/usr/libexec/java_home']).strip().decode('utf-8')

# Set the environment variable in the current Python process
os.environ['JAVA_HOME'] = java_home

# Verify the environment variable
print('JAVA_HOME:', os.environ['JAVA_HOME'])

_____



import subprocess

# Define the command to export JAVA_HOME
command = 'export JAVA_HOME=$(/usr/libexec/java_home)'

# Run the command using subprocess
process = subprocess.Popen(command, shell=True, executable='/bin/bash')
process.communicate()

# Print JAVA_HOME to verify (this will not reflect the change in the current Python process)
process = subprocess.Popen('echo $JAVA_HOME', shell=True, executable='/bin/bash')
stdout, stderr = process.communicate()
print('JAVA_HOME:', stdout.decode().strip())



export JAVA_HOME=$(/usr/libexec/java_home)
export PATH=$JAVA_HOME/bin:$PATH

Step 3: Set JAVA_HOME PermanentlyTo make this change permanent, you need to add the export command to your shell profile file. Depending on the shell you are using (bash or zsh), this will be either .bash_profile or .zshrc.For Bash UsersOpen .bash_profile in a text editor:nano ~/.bash_profileAdd the following line to the file:export JAVA_HOME=$(/usr/libexec/java_home)Save the file and exit the editor (in nano, press Ctrl+X, then Y, then Enter).Apply the changes by sourcing the file:source ~/.bash_profile




export JAVA_HOME=$(/usr/libexec/java_home)

from pyspark.sql import SparkSession

# Define your AWS credentials
aws_access_key_id = "YOUR_ACCESS_KEY_ID"
aws_secret_access_key = "YOUR_SECRET_ACCESS_KEY"

# Create a Spark session
spark = SparkSession.builder \
    .appName("PySpark AWS S3 Example") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .getOrCreate()

# Example: Read a Parquet file from S3
s3_file_path = "s3a://your_bucket/your_file.parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(s3_file_path)

# Show the DataFrame
df.show()



from pyspark.sql import SparkSession

# Define your AWS credentials
aws_access_key_id = "YOUR_ACCESS_KEY_ID"
aws_secret_access_key = "YOUR_SECRET_ACCESS_KEY"

# Create a Spark session with Hadoop AWS and AWS Java SDK dependencies
spark = SparkSession.builder \
    .appName("PySpark AWS S3 Example") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .getOrCreate()

# Example: Read a Parquet file from S3
s3_file_path = "s3a://your_bucket/your_file.parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(s3_file_path)

# Show the DataFrame
df.show()

_______

from pyspark.sql import SparkSession

# Define your AWS credentials
aws_access_key_id = "YOUR_ACCESS_KEY_ID"
aws_secret_access_key = "YOUR_SECRET_ACCESS_KEY"

# Specify the paths to the downloaded JAR files
hadoop_aws_jar = "/path/to/hadoop-aws-3.3.1.jar"
aws_java_sdk_jar = "/path/to/aws-java-sdk-bundle-1.11.901.jar"

# Create a Spark session with the local JAR files
spark = SparkSession.builder \
    .appName("PySpark AWS S3 Example") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.jars", f"{hadoop_aws_jar},{aws_java_sdk_jar}") \
    .getOrCreate()

# Example: Read a Parquet file from S3
s3_file_path = "s3a://your_bucket/your_file.parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(s3_file_path)

# Show the DataFrame
df.show()

