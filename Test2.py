
from pyspark.sql import SparkSession
from pyspark.sql.functions import when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("UpdateTokenization") \
    .getOrCreate()

# Read the Parquet file
file_path = "path/to/your/parquet/file"  # Replace with your file path
df = spark.read.parquet(file_path)

# Update the tokenization column where attribute is 'Consumer Account Number'
df = df.withColumn("tokenization", 
                   when(df["attribute"] == "Consumer Account Number", "PAN").otherwise(df["tokenization"]))

# Save the updated DataFrame as a new Parquet file
output_path = "path/to/save/updated_file.parquet"  # Replace with the desired output path
df.write.parquet(output_path)

# Display the schema and first few rows of the updated DataFrame
df.printSchema()
df.show()
