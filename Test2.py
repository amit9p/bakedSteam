
from pyspark.sql import functions as F

# df1: big table (has many cols incl. identification_number)
# df2: small table (cols: consumer_account_number, identification_number)

# make sure join key types match
key_type = dict(df1.dtypes)["consumer_account_number"]
df2_k = df2.select(
    F.col("consumer_account_number").cast(key_type).alias("consumer_account_number"),
    F.col("identification_number").alias("identification_number_new")
)

# left-join and overwrite the struct where df2 has a value
df_out = (
    df1.alias("l")
       .join(df2_k.alias("r"), on="consumer_account_number", how="left")
       .withColumn(
           "identification_number",
           F.coalesce(F.col("r.identification_number_new"), F.col("l.identification_number"))
       )
       .drop("identification_number_new")
)

# df_out now = all columns from df1 + id_number coming from df2 when available



df_out = (
    df1.alias("l")
       .join(df2_k.alias("r"), "consumer_account_number", "left")
       .drop("l.identification_number")
       .withColumnRenamed("identification_number_new", "identification_number")
)
```0

