

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Example").getOrCreate()

# Assuming df1 and df2 are already loaded as PySpark DataFrames and not pandas DataFrames
# Example loading if needed (you would normally load from a file or other source):
# df1 = spark.createDataFrame([(1, 100), (2, 200), (3, 300)], ["account_id", "value"])
# df2 = spark.createDataFrame([(1, 110), (2, 210), (3, 310)], ["account_id", "value"])

# Join the DataFrames on 'account_id'
final_df = df1.join(df2, df1["account_id"] == df2["account_id"], "inner") \
              .select(df1["account_id"], df1["value"].alias("value_original"), df2["value"].alias("value_manipulated"))

# Show the final DataFrame
final_df.show()
