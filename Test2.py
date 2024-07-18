
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Drop Duplicates").getOrCreate()

# Sample data for df1 and df2
data_df1 = [
    ("1000000000321616", "USTAXID"),
    ("1000000000321636", "PAN"),
    ("1000000000321636", "USTAXID"),
    ("1000000000323351", "PAN")
]
columns_df1 = ["account_number", "tokenization"]

data_df2 = [
    ("1608097536635870836", "Social Security Number", "kxWQmI6m", "USTAXID"),
    ("6494745652255875313", "Social Security Number", "KXK7DW0V", "USTAXID"),
    ("880133357772732421", "Consumer Account Number", "880133zeQc1o23421", "PAN"),
    ("591359754282043895", "Consumer Account Number", "5913593ecT8JA3895", "PAN")
]
columns_df2 = ["account_number", "attribute", "formatted", "tokenization"]

# Create DataFrames
df1 = spark.createDataFrame(data_df1, columns_df1)
df2 = spark.createDataFrame(data_df2, columns_df2)

# Register DataFrames as temp views
df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")

# Perform the join, select specified columns, drop duplicates, and order by account_number
result_df = df1.join(df2, on="tokenization", how="inner") \
    .select(df1.account_number, df2.attribute, df2.formatted, df2.tokenization) \
    .dropDuplicates() \
    .orderBy(df1.account_number)

# Show the result DataFrame
result_df.show(truncate=False)

# Stop the Spark session
spark.stop()
