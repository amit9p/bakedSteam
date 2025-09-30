


from pyspark.sql import functions as F

# Drop sdp4_metadata
df_new = df.drop("sdp4_metadata")

# Add instnc_id as a string column
# You can set a constant value or derive it dynamically
df_new = df_new.withColumn("instnc_id", F.lit("20250930"))   # example string

# Take only 10 rows
df_sample = df_new.limit(10)

# Write to a single parquet file
(df_sample
    .coalesce(1)  # single output file
    .write
    .mode("overwrite")
    .parquet("s3://your-bucket/output/metro2_sample/"))
