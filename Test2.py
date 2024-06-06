

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("FindUniqueAccountID").getOrCreate()

# Assuming df is your DataFrame
df = spark.read.format("parquet").load("path/to/your/parquet/file")

# Filter the DataFrame
filtered_df = df.filter(df.attribute == "social security number")

# Select the account_id and value columns and drop duplicates
unique_accounts = filtered_df.select("account_id", "value").dropDuplicates(["account_id"])

# Show the result
unique_accounts.show()
