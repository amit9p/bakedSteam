
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

# Sample DataFrame
transaction_history = spark.createDataFrame([
    (7777771001, 'PAYMENT', -50.0, '2014-02-14', 1),
    (7777771002, 'PAYMENT', -50.0, '2014-02-28', 2),
    (7777771003, 'PAYMENT', -50.0, '2014-03-14', 1),
    (7777771004, 'PAYMENT', -50.0, '2014-03-28', 2),
    (7777771005, 'PAYMENT', -50.0, '2014-04-11', 1)
], ['account_id', 'transaction_category', 'transaction_amount', 'transaction_date', 'recap_sequence'])

# Mapping logic: update specific account_ids
updated_df = transaction_history.withColumn(
    "account_id",
    F.when(F.col("account_id") == 7777771001, 1044765111)
     .when(F.col("account_id") == 7777771002, 1044765186)
     .when(F.col("account_id") == 7777771003, 1044765130)
     .when(F.col("account_id") == 7777771004, 1044765188)
     .when(F.col("account_id") == 7777771005, 1044765195)
     .otherwise(F.col("account_id"))
)

updated_df.show(truncate=False)
