
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, when

# Initialize Spark session
spark = SparkSession.builder.appName("AddSpacesToPAN").getOrCreate()

# Assuming result_df is already created from the previous code
# Add 13 spaces to the end of each pan number
result_df = result_df.withColumn("pan", concat(col("pan"), lit(" " * 13)))

# Add the tokenization_type field
result_df = result_df.withColumn(
    "tokenization_type",
    when(col("pan").isNotNull(), lit("PAN"))
    .when(col("ssn").isNotNull(), lit("USTAXID"))
    .otherwise(lit(None))
)

# Show the updated DataFrame
result_df.show(1000, False)

# Save the updated DataFrame to Parquet
output_path = "output/pan_ustax_id_1"
result_df.coalesce(1).write.mode("overwrite").parquet(output_path)
