
from pyspark.sql import functions as F

# df1 = big table (many cols incl. identification_number)
# df2 = small table (consumer_account_number, identification_number)

# 1) make sure join-key dtypes match
key_type = dict(df1.dtypes)["consumer_account_number"]  # e.g. 'string' or 'int'

df2_k = (
    df2.select(
        F.col("consumer_account_number").cast(key_type).alias("consumer_account_number"),
        F.col("identification_number").alias("id_new")   # rename to avoid ambiguity
    )
)

# 2) Build final DF: keep ALL cols from df1, but swap the struct if df2 has it
cols_keep = [c for c in df1.columns if c != "identification_number"]

df_updated = (
    df1.alias("l")
      .join(F.broadcast(df2_k).alias("r"), on="consumer_account_number", how="left")
      .select(
          *[F.col(f"l.{c}") for c in cols_keep],                               # all other df1 columns
          F.coalesce(F.col("r.id_new"), F.col("l.identification_number"))       # updated struct
           .alias("identification_number")
      )
)

# sanity check
df_updated.select("consumer_account_number","identification_number").show(100, False)

# write if needed
# df_updated.write.mode("overwrite").parquet("/path/out/accounts_updated.parquet")
