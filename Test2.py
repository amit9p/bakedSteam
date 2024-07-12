
from pyspark.sql import SparkSession
from pyspark.sql.functions import length, col

# Initialize Spark session
spark = SparkSession.builder.appName("GetValueLength").getOrCreate()

# Load the DataFrame from the Parquet file
df1 = spark.read.parquet("/mnt/data/file-2341N8Q5Ck9XWceafQThIIBc")

# Filter the DataFrame for the specified attributes
df1_filtered = df1.filter(
    (col("attribute") == "Social Security Number") | (col("attribute") == "Consumer Account Number")
)

# Add a length column to the filtered DataFrame
df1_with_length = df1_filtered.withColumn("value_length", length(col("value")))

# Show the filtered DataFrame with the value length
df1_with_length.show()
