
from pyspark.sql import SparkSession

# Create a Spark session with initial configurations
spark = (
    SparkSession.builder.appName("PySpark AWS S3 Example")
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
    .config("spark.hadoop.fs.s3a.session.token", aws_session_token)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", True)
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.jars.ivySettings", SPARK_JARS_IVYSETTINGS)
    .config("spark.jars.packages", SPARK_JARS_PACKAGES)
    .getOrCreate()
)

# Add more configurations after the Spark session is created
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.driver.memory", "2g")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Verify the new configurations
print(spark.conf.get("spark.executor.memory"))  # should print '4g'
print(spark.conf.get("spark.driver.memory"))    # should print '2g'
print(spark.conf.get("spark.sql.shuffle.partitions"))  # should print '200'
