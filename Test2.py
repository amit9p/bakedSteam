

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("UpdateColumnNames") \
    .getOrCreate()

# Read the Parquet file
df = spark.read.parquet("path/to/your/parquet/file")

# Rename columns
df = df.withColumnRenamed("account_id", "account_number") \
       .withColumnRenamed("value", "formatted") \
       .withColumnRenamed("tokenization_type", "tokenization")

# Save the updated DataFrame as a new Parquet file
df.write.parquet("path/to/save/new_parquet/file")

# Stop the Spark session
spark.stop()
