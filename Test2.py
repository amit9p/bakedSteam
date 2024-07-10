
from pyspark.sql import SparkSession
import shutil
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Single Parquet File Without CRC Example") \
    .getOrCreate()

# Sample DataFrame
data = [("James", "Smith", "USA", "CA"), 
        ("Michael", "Rose", "USA", "NY"), 
        ("Robert", "Williams", "USA", "CA"), 
        ("Maria", "Jones", "USA", "FL")]

columns = ["firstname", "lastname", "country", "state"]

df = spark.createDataFrame(data, columns)

# Configure Hadoop to disable CRC
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("parquet.enable.summary-metadata", "false")
hadoop_conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
hadoop_conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
hadoop_conf.set("fs.file.impl.disable.cache", "true")

# Define output path
output_path = '/path/to/output/single_parquet_file'

# Write to a single Parquet file
df.coalesce(1).write.mode('overwrite').parquet(output_path)

# Remove the _SUCCESS file if it exists
success_file_path = os.path.join(output_path, "_SUCCESS")
if os.path.exists(success_file_path):
    os.remove(success_file_path)

# Remove the CRC files if they exist
crc_files = [f for f in os.listdir(output_path) if f.endswith('.crc')]
for crc_file in crc_files:
    os.remove(os.path.join(output_path, crc_file))

# Stop the SparkSession
spark.stop()
