
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("UpdateFormatted") \
    .getOrCreate()

# Read the Parquet files
df1 = spark.read.parquet("/mnt/data/file-B9fH04dCsxaonewrNoPlpCt4")  # Replace with actual path for df1
df2 = spark.read.parquet("/mnt/data/file-cX64QyV4WYLd38efAHVrzFd4")  # Replace with actual path for df2

# Select only the columns needed from df2
df2_selected = df2.select(
    col("attribute").alias("df2_attribute"), 
    col("tokenization").alias("df2_tokenization"), 
    col("formatted").alias("df2_formatted")
)

# Define the condition for the join and the update
condition = (
    (df1["attribute"] == df2_selected["df2_attribute"]) & 
    (df1["tokenization"] == df2_selected["df2_tokenization"])
)

# Perform the join
df_joined = df1.join(df2_selected, condition, "left")

# Update only the `formatted` column
df1_updated = df_joined.withColumn(
    "formatted", 
    when(
        (col("attribute").isin("Social Security Number", "Consumer Account Number")) & 
        (col("tokenization").isin("USTAXID", "PAN")) & 
        col("df2_formatted").isNotNull(), 
        col("df2_formatted")
    ).otherwise(col("formatted"))
).select(df1.columns)  # Select only the original columns to exclude the extra ones

# Save the updated DataFrame as a new Parquet file
output_path = "/mnt/data/updated_file.parquet"  # Replace with the desired output path
df1_updated.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()
