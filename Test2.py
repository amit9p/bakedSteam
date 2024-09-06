
from pyspark.sql.functions import col

# Update data types as per the schema in the second image
df_final = df_final.select(
    col("df_a.business_date").cast("date").alias("business_date"),  # DATE
    col("df_a.run_identifier").cast("int").alias("run_identifier"),  # INTEGER
    col("df_a.output_file_type").cast("string").alias("output_filetype"),  # STRING
    col("df_a.output_record_sequence").cast("int").alias("output_record_sequence"),  # INTEGER
    col("df_a.output_field_sequence").cast("int").alias("output_field_sequence"),  # INTEGER
    col("df_a.attribute").cast("string").alias("attribute"),  # STRING
    col("final_formatted").cast("string").alias("formatted"),  # STRING
    col("df_a.tokenization").cast("string").alias("tokenization"),  # NULLABLE STRING
    col("df_a.account_number").cast("string").alias("account_number"),  # NULLABLE STRING
    col("df_a.segment").cast("string").alias("segment")  # STRING
)

# Print schema and show some data for verification
df_final.printSchema()
df_final.show(n=500, truncate=False)
