from pyspark.sql.functions import col
from pyspark.sql.types import LongType, IntegerType, DateType, DoubleType

# Assuming df is your original DataFrame with all 9 columns
df_converted = (
    df.withColumn("account_id", col("account_id").cast(LongType()))
      .withColumn("recap_sequence", col("recap_sequence").cast(IntegerType()))
      .withColumn("transaction_posting_date", col("transaction_posting_date").cast(DateType()))
      .withColumn("transaction_date", col("transaction_date").cast(DateType()))
      .withColumn("transaction_category", col("transaction_category").cast("string"))
      .withColumn("transaction_source", col("transaction_source").cast("string"))
      .withColumn("transaction_description", col("transaction_description").cast("string"))
      .withColumn("transaction_amount", col("transaction_amount").cast(DoubleType()))
      .withColumn("transaction_resulting_balance", col("transaction_resulting_balance").cast(DoubleType()))
)

# Print schema to verify
df_converted.printSchema()
