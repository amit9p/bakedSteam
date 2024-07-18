

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

# Initialize Spark session
spark = SparkSession.builder.appName("Filter Rows").getOrCreate()

# Sample data for result_df
data_result = [
    ("1000000000321616", "Consumer Account Number", "880133zeQc1o23421", "PAN"),
    ("1000000000321616", "Consumer Account Number", "5913593ecT8JA3895", "PAN"),
    ("1000000000321616", "Social Security Number", "kxWQmIG6m", "USTAXID"),
    ("1000000000321616", "Social Security Number", "KXK7DW0V", "USTAXID"),
    ("1000000000323351", "Consumer Account Number", "880133zeQc1o23421", "PAN"),
    ("1000000000323351", "Consumer Account Number", "5913593ecT8JA3895", "PAN"),
    ("1000000000323351", "Social Security Number", "kxWQmIG6m", "USTAXID"),
    ("1000000000323351", "Social Security Number", "KXK7DW0V", "USTAXID")
]
columns_result = ["account_number", "attribute", "formatted", "tokenization"]

# Create DataFrame
result_df = spark.createDataFrame(data_result, columns_result)

# Define window specification
window_spec = Window.partitionBy("account_number", "tokenization").orderBy("account_number")

# Add row number based on the window specification
result_df_with_row_num = result_df.withColumn("row_num", row_number().over(window_spec))

# Filter to keep only the first 2 rows for each account_number and tokenization
filtered_df = result_df_with_row_num.filter(col("row_num") <= 2).drop("row_num")

# Show the result DataFrame
filtered_df.show(truncate=False)

# Stop the Spark session
spark.stop()
