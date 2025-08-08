
from pyspark.sql import functions as F

orig_cols = df1.columns  # preserves df1's column order

df2_k = (df2.select(
            F.col("consumer_account_number").cast(dict(df1.dtypes)["consumer_account_number"]).alias("consumer_account_number"),
            F.col("identification_number").alias("id_new"))
        )

df_updated = (
    df1.alias("l")
      .join(F.broadcast(df2_k).alias("r"), "consumer_account_number", "left")
      .select(*[
          # keep df1 order; replace only this column
          F.coalesce(F.col("r.id_new"), F.col("l.identification_number")).alias("identification_number")
          if c == "identification_number" else F.col(f"l.{c}")
          for c in orig_cols
      ])
)

# df_updated now has the SAME column order as df1, with identification_number in its original position.
