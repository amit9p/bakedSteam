





from pyspark.sql import Row
from pyspark.sql import functions as F

# Drop instnc_id if present
df_no_instnc = df.drop("instnc_id")

# Use zipWithIndex to create sequential row numbers
df_indexed = df_no_instnc.rdd.zipWithIndex().map(
    lambda x: Row(**x[0].asDict(), row_num=x[1] + 1)
).toDF()

# Generate account_id starting from 7777771001
df_new = df_indexed.withColumn(
    "account_id", (F.lit(7777771000) + F.col("row_num")).cast("string")
).drop("row_num")

# Show first 20 rows
df_new.show(20, truncate=False)

# (Optional) Write to Parquet
(df_new
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet("s3://your-bucket/output/df_with_new_account_ids/"))
