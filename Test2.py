
from pyspark.sql import SparkSession

# Sample list of account IDs to filter
account_ids = ['A1001', 'A1003', 'A1005']

# Assuming df is your DataFrame and it has a column named 'account_id'
filtered_df = df.filter(df['account_id'].isin(account_ids))

# Show the result
filtered_df.show()


№######
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder.appName("ParseJSON").getOrCreate()

# If reading from file
with open("/mnt/data/file-Jk1NVM8oNoiiygAPFDvxcT", "r") as file:
    json_text = file.read()

# Parse as JSON list
json_data = json.loads(json_text)

# Create Spark DataFrame
df = spark.read.json(spark.sparkContext.parallelize(json_data))

# Filter where result == "FAIL"
failed_df = df.filter(col("result") == "FAIL")

# Select relevant columns
failed_df.select("executionTimestamp", "ruleId", "fieldName", "result").show(truncate=False)
