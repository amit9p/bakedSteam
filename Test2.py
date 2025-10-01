

from pyspark.sql import functions as F
from pyspark.sql import Row

# 1. Drop unwanted column
df2 = df.drop("sdp4_metadata")

# 2. Add instnc_id column (static value, or replace with dynamic if needed)
df2 = df2.withColumn("instnc_id", F.lit("20251001"))

# 3. Generate new account_id starting at 7777771001
# Use zipWithIndex to avoid expensive window()
df_with_idx = df2.rdd.zipWithIndex().map(
    lambda x: Row(**x[0].asDict(), row_num=x[1]+1)  # +1 so index starts at 1
).toDF()

df_with_new_id = df_with_idx.withColumn(
    "account_id",
    (F.lit(7777771000) + F.col("row_num")).cast("long")
).drop("row_num")

# 4. Reorder columns: instnc_id second last, chargeoff_principal last
cols = df_with_new_id.columns
cols_reordered = [c for c in cols if c not in ["instnc_id", "chargeoff_principal"]]
cols_reordered = cols_reordered + ["instnc_id", "chargeoff_principal"]

df_final = df_with_new_id.select(cols_reordered)

# Show result
df_final.printSchema()
df_final.show(20, truncate=False)
