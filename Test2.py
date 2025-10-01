


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Drop instnc_id
df_new = df.drop("instnc_id")

# Step 1: Assign a new sequential number per distinct account_id
account_map = (
    df_new
    .select("account_id")
    .distinct()
    .withColumn("row_num", F.row_number().over(Window.orderBy("account_id")))
    .withColumn("new_account_id", (F.lit(7777771000) + F.col("row_num")).cast("string"))
    .drop("row_num")
)

# Step 2: Join back to original DataFrame so all transactions for an account map to same new ID
df_mapped = (
    df_new
    .join(account_map, on="account_id", how="left")
    .drop("account_id")  # drop old one
    .withColumnRenamed("new_account_id", "account_id")  # rename new
)

# Step 3: (Optional) Keep recap sequence ordering inside each account
df_mapped = df_mapped.withColumn(
    "recap_seq",
    F.row_number().over(Window.partitionBy("account_id").orderBy("transaction_date"))
)

# Show result
df_mapped.show(20, truncate=False)
