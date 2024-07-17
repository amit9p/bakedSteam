
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, broadcast, row_number
from pyspark.sql.window import Window

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

# Define window specifications to ensure uniqueness
window_spec_base = Window.partitionBy("account_number", "tokenization").orderBy("segment")
window_spec_j2 = Window.partitionBy("account_number", "tokenization").orderBy("segment")

# Add row numbers to filter unique records
df1_filtered = df1_updated.withColumn(
    "row_num",
    when(
        (col("tokenization") == "PAN") & (col("segment") == "BASE"), 
        row_number().over(window_spec_base)
    ).when(
        (col("tokenization") == "USTAXID") & (col("segment") == "BASE"), 
        row_number().over(window_spec_base)
    ).when(
        (col("tokenization") == "USTAXID") & (col("segment") == "J2"), 
        row_number().over(window_spec_j2)
    ).otherwise(None)
)

# Filter to keep only one record per segment condition
df1_filtered = df1_filtered.filter(
    ((col("tokenization") == "PAN") & (col("segment") == "BASE") & (col("row_num") == 1)) |
    ((col("tokenization") == "USTAXID") & (col("segment") == "BASE") & (col("row_num") == 1)) |
    ((col("tokenization") == "USTAXID") & (col("segment") == "J2") & (col("row_num") == 1)) |
    ((col("tokenization") != "PAN") & (col("tokenization") != "USTAXID"))
).drop("row_num")

# Select only the original columns from df1 to ensure no extra columns are included
df1_final = df1_filtered.select(df1.columns)

# Save the updated DataFrame as a new Parquet file
output_path = "/mnt/data/updated_file.parquet"  # Replace with the desired output path
df1_final.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()
