
from pyspark.sql import functions as F

# 1. Drop sdp4_metadata
df_new = df.drop("sdp4_metadata")

# 2. Add instnc_id as a constant string (you can change "20250930" to today’s date if needed)
df_new = df_new.withColumn("instnc_id", F.lit("20250930"))

# 3. Select only 10 rows
df_sample = df_new.limit(10)

# 4. Replace account_id values starting from 7777771001
df_sample = (
    df_sample
    .withColumn("row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
    .withColumn("account_id", (F.lit(7777771000) + F.col("row_num")).cast("string"))
    .drop("row_num")
)

# 5. Show final result
df_sample.show(truncate=False)

# 6. (Optional) Write output to a single parquet file
(df_sample
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet("s3://your-bucket/output/metro2_sample/"))
