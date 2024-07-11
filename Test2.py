
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("JoinDataFrames").getOrCreate()

# Sample data for df1
data1 = [
    ("23545616256127562", "123-45-6789", "PAN12345A"),
    ("23545616256127563", "987-65-4321", "PAN67890B"),
    # Add more records as needed
]

# Sample data for df2
data2 = [
    ("23545616256127562", "111-22-3333", "PAN99999Z"),
    ("23545616256127563", "444-55-6666", "PAN88888Y"),
    # Add more records as needed
]

# Create DataFrames
df1 = spark.createDataFrame(data1, ["account_id", "ssn", "pan"])
df2 = spark.createDataFrame(data2, ["account_id", "ssn", "pan"])

# Perform the join on account_id
result_df = df1.join(df2, on="account_id", how="inner") \
               .select(df1.account_id, df1.ssn, df2.pan)

# Show the result
result_df.show()

# Stop the Spark session
spark.stop()
