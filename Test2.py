
from pyspark.sql import SparkSession, functions as F

# Initialize Spark
spark = SparkSession.builder.appName("RotateAccountIDs").getOrCreate()

# ---------------------------
# 1️⃣ Create sample DataFrames
# ---------------------------

transaction_history = spark.createDataFrame([
    (7777771001, 'PAYMENT', -50.0, '2014-02-14', 1),
    (7777771002, 'PAYMENT', -50.0, '2014-02-28', 2),
    (7777771003, 'PAYMENT', -50.0, '2014-03-14', 1),
    (7777771004, 'PAYMENT', -50.0, '2014-03-28', 2),
], ['account_id', 'transaction_category', 'transaction_amount', 'transaction_date', 'recap_sequence'])

account_info = spark.createDataFrame([
    (1044765111, 633.03, '2018-11-29', 633.03),
    (1044765186, 606.35, '2023-03-30', 606.35),
    (1044765172, 1972.72, '2023-01-01', 1972.72),
    (1044765198, 589.96, '2021-06-01', 589.96),
], ['account_id', 'total_current_balance', 'pre_chargeoff_last_payment_date', 'chargeoffbalance'])

# ---------------------------
# 2️⃣ Rotate account IDs
# ---------------------------

# Collect all IDs from account_info
ids = [r.account_id for r in account_info.select("account_id").collect()]

# Move last to first, drop original first
rotated_ids = [ids[-1]] + ids[1:]

# Limit rotation to same number of rows as transaction_history (safety)
n_txn = transaction_history.count()
rotated_ids = rotated_ids[:n_txn]

# Create DataFrame of rotated IDs with row_id
rotated_df = (
    spark.createDataFrame([(i,) for i in rotated_ids], ["new_account_id"])
    .withColumn("row_id", F.monotonically_increasing_id())
)

# ---------------------------
# 3️⃣ Add row_id and join
# ---------------------------

# Add unique row_id to transaction_history and rename old account_id
th = (
    transaction_history
    .withColumn("row_id", F.monotonically_increasing_id())
    .withColumnRenamed("account_id", "old_account_id")
)

# Join and keep only new account_id
updated_df = (
    th.join(rotated_df, on="row_id", how="inner")
      .drop("row_id", "old_account_id")
      .withColumnRenamed("new_account_id", "account_id")
      .select("account_id", "transaction_category", "transaction_amount", "transaction_date", "recap_sequence")
)

# ---------------------------
# 4️⃣ Show final output
# ---------------------------
updated_df.show(truncate=False)
