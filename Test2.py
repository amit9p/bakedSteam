
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Replace Value Column") \
    .getOrCreate()

# Load the two DataFrames from the provided file paths
df1 = spark.read.option("header", "true").csv("/mnt/data/file-BeZjudu5JkBPnVKOJ1KQzqTw")
df2 = spark.read.option("header", "true").csv("/mnt/data/file-jEGiRvtcsHK3q8aSabrFf6wY")

# Show the schemas of both DataFrames to understand their structure
df1.printSchema()
df2.printSchema()

# Select the necessary columns from df1 (assuming 'account_id' and 'value')
df1_selected = df1.select("account_id", "value").withColumnRenamed("value", "new_value")

# Perform the left join on 'account_id' to keep all rows from df2
df2_updated = df2.join(df1_selected, on="account_id", how="left")

# Replace the original 'value' column with the new 'value' column from df1
df2_final = df2_updated.withColumn("value", coalesce(df1_selected["new_value"], df2["value"])).drop("new_value")

# Show the result
df2_final.show()

# Stop the Spark session
spark.stop()
