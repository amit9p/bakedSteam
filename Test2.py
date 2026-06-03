from pyspark.sql.functions import col

df_addr = spark.read.parquet(
    "s3a://ecbr-pfm-s3-it-dfsl1-e1/tenant_data/c897ba20-33bd-4d7b-994c-f23e4570272f-DR-04-26-dfsl1-test5/consolidator_outputs/load/account_service_address/"
)

non_ascii_pattern = r'[^\x00-\x7F]'

result_addr = df_addr.filter(
    col("address_line_1").rlike(non_ascii_pattern) |
    col("address_line_2").rlike(non_ascii_pattern) |
    col("city").rlike(non_ascii_pattern)
).select(
    "customer_id", "address_id", "address_line_1", "address_line_2", "city"
)

result_addr.show(truncate=False)
