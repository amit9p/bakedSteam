
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("UpdateFormatted") \
    .getOrCreate()

# Read the Parquet files
df1 = spark.read.parquet("path/to/your/df1/parquet/file")  # Replace with actual path
df2 = spark.read.parquet("path/to/your/df2/parquet/file")  # Replace with actual path

# Select only the columns needed from df2
df2_selected = df2.select("attribute", "tokenization", "formatted")

# Join df1 with df2_selected on attribute and tokenization
condition = (
    (df1["attribute"] == df2_selected["attribute"]) & 
    (df1["tokenization"] == df2_selected["tokenization"])
) & (
    ((df1["attribute"] == "Social Security Number") & (df1["tokenization"].isin("USTAXID", "PAN"))) |
    ((df1["attribute"] == "Consumer Account Number") & (df1["tokenization"].isin("USTAXID", "PAN")))
)

# Perform the update
df1_updated = df1.join(df2_selected, condition, "left") \
    .withColumn("formatted", when(col("df2.formatted").isNotNull(), col("df2.formatted")).otherwise(col("df1.formatted"))) \
    .drop(df2_selected["formatted"])

# Save the updated DataFrame as a new Parquet file
output_path = "path/to/save/updated_file.parquet"  # Replace with the desired output path
df1_updated.write.parquet(output_path)

# Stop the Spark session
spark.stop()
