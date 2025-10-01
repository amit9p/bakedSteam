
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Drop sdp4_metadata
df_new = df.drop("sdp4_metadata")

# 2. Add instnc_id as a constant string (you can change this as needed)
df_new = df_new.withColumn("instnc_id", F.lit("20250930"))

# 3. Select only 10 rows
df_sample = df_new.limit(10)

# 4. Add sequential account_id starting from 7777771001
df_sample = (
    df_sample
    .withColumn("row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
    .withColumn("account_id", (F.lit(7777771000) + F.col("row_num")).cast("string"))
)

# 5. Update reporting_status so that half rows = "R", rest = "T"
df_sample = df_sample.withColumn(
    "reporting_status",
    F.when(F.col("row_num") % 2 == 0, F.lit("R")).otherwise(F.lit("T"))
)

# 6. Drop helper column
df_sample = df_sample.drop("row_num")

# 7. Show final 10 records
df_sample.show(truncate=False)

# 8. (Optional) Write to single parquet file
(df_sample
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet("s3://your-bucket/output/metro2_sample/"))
