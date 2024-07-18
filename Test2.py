

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Delete Alternate Rows").getOrCreate()

# Sample data for result_df
data_result = [
    ("1000000000321616", "Consumer Account Number", "880133zeQc1o23421", "PAN"),
    ("1000000000321616", "Consumer Account Number", "5913593ecT8JA3895", "PAN"),
    ("1000000000323351", "Consumer Account Number", "880133zeQc1o23421", "PAN"),
    ("1000000000323351", "Consumer Account Number", "5913593ecT8JA3895", "PAN"),
    ("1000000000323351", "Social Security Number", "kxWQmIG6m", "USTAXID"),
    ("1000000000323351", "Social Security Number", "KXK7DWG0V", "USTAXID"),
    ("1000000000321616", "Social Security Number", "kxWQmIG6m", "USTAXID"),
    ("1000000000321616", "Social Security Number", "KXK7DWG0V", "USTAXID")
]
columns_result = ["account_number", "attribute", "formatted", "tokenization"]

# Create DataFrame
result_df = spark.createDataFrame(data_result, columns_result)

# Add a unique row number to each row
window = Window.orderBy(monotonically_increasing_id())
result_df_with_index = result_df.withColumn("row_num", row_number().over(window))

# Filter out alternate rows (e.g., keep rows with even row_num values)
filtered_df = result_df_with_index.filter((col("row_num") % 2) == 0).drop("row_num")

# Show the result DataFrame
filtered_df.show(truncate=False)

# Stop the Spark session
spark.stop()
