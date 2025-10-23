
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

# Example DataFrame
df = spark.createDataFrame([
    (1044765111, 1039593071, '1J', None),
    (1044765186, 1039592951, '1J', None),
    (1044765161, 1039592991, '1J', None)
], ['account_id', 'customer_id', 'ownership_type_ecoa', 'consumer_info_indicator'])

# ✅ Add new column 'customer_pk_id' with same value as account_id
df = df.withColumn("customer_pk_id", F.col("account_id").cast("long"))

# ✅ Reorder columns so 'customer_pk_id' is first
final_df = df.select(
    "customer_pk_id",
    "account_id",
    "customer_id",
    "ownership_type_ecoa",
    "consumer_info_indicator"
)

# Show result
final_df.show(truncate=False)
final_df.printSchema()
