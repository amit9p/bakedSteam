
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Single Parquet File Example") \
    .getOrCreate()

# Sample DataFrame
data = [("James", "Smith", "USA", "CA"), 
        ("Michael", "Rose", "USA", "NY"), 
        ("Robert", "Williams", "USA", "CA"), 
        ("Maria", "Jones", "USA", "FL")]

columns = ["firstname", "lastname", "country", "state"]

df = spark.createDataFrame(data, columns)

# Write to a single Parquet file
df.coalesce(1).write.mode('overwrite').parquet('/path/to/output/single_parquet_file')

# Stop the SparkSession
spark.stop()
