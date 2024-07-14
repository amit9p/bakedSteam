
from pyspark.sql import SparkSession
import os

# Initialize Spark session
spark = SparkSession.builder.appName("WriteParquetWithoutSuccessAndCrc").getOrCreate()

# Set Hadoop configuration to avoid generating the _SUCCESS file
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# Create a sample DataFrame
data = [("1", "Social Security Number", "123456789", "USTAXID"),
        ("2", "Consumer Account Number", "987654321", "PAN")]
schema = ["account_id", "attribute", "value", "tokenization_type"]
df = spark.createDataFrame(data, schema)

# Path to save the Parquet file
output_path = "/path/to/your/output/directory"

# Write the DataFrame to Parquet file
df.write.mode("overwrite").parquet(output_path)

# Remove .crc files
os.system(f"hadoop fs -rm {output_path}/*.crc")
