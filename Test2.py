

from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col

# Initialize Spark session
spark = SparkSession.builder.appName("GetValueLength").getOrCreate()

# Load the DataFrame from the Parquet file
df1 = spark.read.parquet("/mnt/data/file-2341N8Q5Ck9XWceafQThIIBc")

# Filter the DataFrame for the specified attributes and add a length column
df1_filtered = df1.filter(
    (col("attribute") == "Social Security Number") | (col("attribute") == "Consumer Account Number")
).withColumn("value_length", length(col("value")))

# Show the filtered DataFrame with the value length
df1_filtered.show()
