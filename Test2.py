


from pyspark.sql import functions as F, types as T
from pyspark.sql import Row

# ---- 1) Build rows using only strings / plain numbers ----
rows = [
    Row(account_id=7777771001, recap_sequence=1,
        transaction_posting_date="2020-02-26", transaction_date="2020-02-26",
        transaction_category="PAYMENT", transaction_source="IEPS",
        transaction_description="175292049842333844342",
        transaction_amount="-150.0", transaction_resulting_balance="6555.28"),
    Row(account_id=7777771001, recap_sequence=2,
        transaction_posting_date="2020-02-26", transaction_date="2020-02-26",
        transaction_category="PAYMENT", transaction_source="IEPS",
        transaction_description="175292049842333844342",
        transaction_amount="-150.0", transaction_resulting_balance="6405.28"),
    # ... add the rest of your rows the same way ...
]

# ---- 2) Create DF without forcing a complex schema upfront ----
df_raw = spark.createDataFrame(rows)

# ---- 3) Cast to your catalog schema inside Spark ----
df = (df_raw
      .withColumn("account_id", F.col("account_id").cast(T.LongType()))
      .withColumn("recap_sequence", F.col("recap_sequence").cast(T.IntegerType()))
      .withColumn("transaction_posting_date", F.to_date("transaction_posting_date", "yyyy-MM-dd"))
      .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
      .withColumn("transaction_category", F.col("transaction_category").cast(T.StringType()))
      .withColumn("transaction_source", F.col("transaction_source").cast(T.StringType()))
      .withColumn("transaction_description", F.col("transaction_description").cast(T.StringType()))
      .withColumn("transaction_amount", F.col("transaction_amount").cast(T.DoubleType()))
      .withColumn("transaction_resulting_balance", F.col("transaction_resulting_balance").cast(T.DoubleType()))
)

df.printSchema()
df.show(truncate=False)
