

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col

# Create Spark session
spark = SparkSession.builder.appName("UpdateDateClosed").getOrCreate()

# Read CSV file
df = spark.read.option("header", True).csv("/Users/vmq634/.../your_input_file.csv")

# Update some values in date_closed
df_updated = df.withColumn(
    "date_closed",
    when(col("consumer_account_number") == "12345", lit("1900-01-01"))  # Example condition
    .when(col("consumer_account_number") == "67890", lit("1899-12-31"))  # Another example
    .otherwise(col("date_closed"))  # Keep original
)

# Write to new CSV
df_updated.write.mode("overwrite").option("header", True).csv("/Users/vmq634/.../updated_output.csv")
