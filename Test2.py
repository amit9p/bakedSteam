
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrame Join Example").getOrCreate()

# Load your data from files (assuming they are CSVs for this example)
df1 = spark.read.option("header", "true").csv("/mnt/data/file-TgrN09cq2nytAtxdrIaIKeVx")
df2 = spark.read.option("header", "true").csv("/mnt/data/file-k2LYBUeR7XuM7LQiR8zkkVOt")

# Show the content of df1 and df2
df1.show(truncate=False)
df2.show(truncate=False)

# Perform the join on both account_number and tokenization
result_df = df1.join(df2, (df1.tokenization == df2.tokenization) & (df1.account_number == df2.account_number), how="inner")\
               .select(df1.account_number.alias("account_number"), df2.attribute, df2.formatted, df1.tokenization)

# Show the result
result_df.show(truncate=False)
