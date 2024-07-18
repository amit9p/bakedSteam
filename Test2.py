
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Combine DataFrame Columns").getOrCreate()

# Sample data for df1 and df2
data1 = [
    ("1000000000323351", "USTAXID"),
    ("1000000000321636", "PAN"),
    ("1000000000321636", "USTAXID"),
    ("1000000000323351", "PAN")
]
columns1 = ["account_number", "tokenization"]

data2 = [
    ("1608097536635870836", "Social Security Number", "kxWQmI6m", "USTAXID"),
    ("6494745652255875313", "Social Security Number", "KXK7DW0V", "USTAXID"),
    ("880133357772732421", "Consumer Account Number", "880133zeQc1o23421", "PAN"),
    ("591359754282043895", "Consumer Account Number", "5913593ecT8JA3895", "PAN")
]
columns2 = ["account_number", "attribute", "formatted", "tokenization"]

# Create DataFrames
df1 = spark.createDataFrame(data1, columns1)
df2 = spark.createDataFrame(data2, columns2)

# Perform the join
result_df = df1.join(df2, on="tokenization", how="inner") \
    .select(df1.account_number, df2.attribute, df2.formatted, df2.tokenization)

# Remove duplicates based on account_number and tokenization
result_df = result_df.dropDuplicates(["account_number", "tokenization"])

# Show the result DataFrame
result_df.show(truncate=False)

# Stop the Spark session
spark.stop()
