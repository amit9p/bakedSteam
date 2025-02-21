
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("ParquetFileExample").getOrCreate()

# Define schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False)
])

# Prepare data
data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 35)
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Write to Parquet file
df.write.mode("overwrite").parquet("output.parquet")

# Stop Spark session
spark.stop()

print("Parquet file created successfully.")
