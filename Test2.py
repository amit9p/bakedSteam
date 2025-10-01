

from pyspark.sql import Row, functions as F

# Step 1: Add sequential row numbers using zipWithIndex
df_indexed = df.rdd.zipWithIndex().map(
    lambda x: Row(**x[0].asDict(), rn=x[1] + 1)  # +1 so it starts from 1
).toDF()

# Step 2: Replace account_id with sequential values starting from 7777771001
df_new = df_indexed.withColumn(
    "account_id", (F.lit(7777771000) + F.col("rn")).cast("long")
).drop("rn")

# Step 3: Reorder columns → move chargeoff_principal to the end
cols = df_new.columns
cols_reordered = [c for c in cols if c != "chargeoff_principal"] + ["chargeoff_principal"]

df_final = df_new.select(cols_reordered)

# Show results
df_final.printSchema()
df_final.show(10, truncate=False)
