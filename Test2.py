

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Update DataFrame").getOrCreate()

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

# Perform the join and filter out duplicates
df2_updated = df2.join(df1, on="tokenization", how="left") \
    .drop(df2.account_number) \
    .withColumnRenamed("account_number", "updated_account_number")

# Remove duplicates if needed
df2_updated = df2_updated.drop_duplicates()

# Show the updated DataFrame
df2_updated.show(truncate=False)

# Stop the Spark session
spark.stop()
