

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Drop instnc_id
df_new = df.drop("instnc_id")

# 2. Add row number to generate sequential account_id
df_new = (
    df_new
    .withColumn("row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
    .withColumn("account_id", (F.lit(7777771000) + F.col("row_num")).cast("string"))
    .drop("row_num")
)

# 3. Show sample
df_new.show(20, truncate=False)

# 4. (Optional) Write back to parquet
(df_new
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet("s3://your-bucket/output/df_with_new_account_ids/"))
