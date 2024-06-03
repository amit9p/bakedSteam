

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

