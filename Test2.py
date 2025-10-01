

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 1: Add sequential row numbers using row_number()
w = Window.orderBy(F.monotonically_increasing_id())

df_indexed = df.withColumn("rn", F.row_number().over(w))

# Step 2: Replace account_id with sequential values starting from 7777771001
df_new = df_indexed.withColumn(
    "account_id",
    (F.lit(7777771000) + F.col("rn")).cast("long")
).drop("rn")

# Step 3: Reorder columns: move chargeoff_principal to the end
cols = df_new.columns
cols_reordered = [c for c in cols if c != "chargeoff_principal"] + ["chargeoff_principal"]

df_final = df_new.select(cols_reordered)

# Show final result
df_final.show(10, truncate=False)
