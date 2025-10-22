
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

spark = SparkSession.builder.getOrCreate()

# Example DataFrames (replace with your actual DataFrames)
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

# Add a unique ID to preserve row order for replacement
transaction_history = transaction_history.withColumn("row_id", monotonically_increasing_id())
account_info = account_info.withColumn("row_id", monotonically_increasing_id())

# Replace account_id values using row_id alignment
updated_df = (
    transaction_history
    .join(account_info.select("row_id", account_info.account_id.alias("new_account_id")), on="row_id", how="inner")
    .drop("row_id")
    .withColumnRenamed("new_account_id", "account_id")
)

updated_df.show(truncate=False)
