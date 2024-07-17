
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, broadcast

# Initialize Spark session
spark = SparkSession.builder \
    .appName("UpdateFormatted") \
    .getOrCreate()

# Read the Parquet files
df1 = spark.read.parquet("/mnt/data/file-B9fH04dCsxaonewrNoPlpCt4")  # Replace with actual path for df1
df2 = spark.read.parquet("/mnt/data/file-cX64QyV4WYLd38efAHVrzFd4")  # Replace with actual path for df2

# Filter df2 to only include necessary records
df2_filtered = df2.filter(
    (df2["attribute"].isin("Social Security Number", "Consumer Account Number")) & 
    (df2["tokenization"].isin("USTAXID", "PAN"))
).select(
    col("attribute").alias("df2_attribute"), 
    col("tokenization").alias("df2_tokenization"), 
    col("formatted").alias("df2_formatted")
)

# Broadcast the filtered df2 to avoid shuffle and join explosion
df2_broadcast = broadcast(df2_filtered)

# Perform the join with df1
df_joined = df1.alias("df1").join(
    df2_broadcast,
    (df1["attribute"] == df2_broadcast["df2_attribute"]) & 
    (df1["tokenization"] == df2_broadcast["df2_tokenization"]),
    "left"
)

# Perform the update on the formatted column based on the conditions
df1_updated = df_joined.withColumn(
    "formatted", 
    when(
        col("df2_formatted").isNotNull(), 
        col("df2_formatted")
    ).otherwise(col("df1.formatted"))
)

# Select only the original columns from df1 to ensure no extra columns are included
df1_updated = df1_updated.select(df1.columns)

# Save the updated DataFrame as a new Parquet file
output_path = "/mnt/data/updated_file.parquet"  # Replace with the desired output path
df1_updated.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()
