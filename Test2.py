from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("DataFrame Join Example").getOrCreate()

# Sample data for DataFrame 1
data1 = [("val1", "123", "USTAXID"), ("val2", "456", "PAN")]
df1 = spark.createDataFrame(data1, ["value", "account_id", "tokenization_type"])

# Sample data for DataFrame 2
data2 = [("val3", "123", "USTAXID"), ("val4", "456", "PAN")]
df2 = spark.createDataFrame(data2, ["value", "account_id", "tokenization_type"])

# Rename the 'value' column in df1 and df2 to make them distinguishable
df1_renamed = df1.withColumnRenamed("value", "value_df1")
df2_renamed = df2.withColumnRenamed("value", "value_df2")

# Perform an inner join on 'account_id' and 'tokenization_type'
df_joined = df1_renamed.join(df2_renamed, ["account_id", "tokenization_type"], "inner")

# Show the resulting DataFrame
df_joined.show()

# Optionally, if you want to rearrange the columns in specific order
df_final = df_joined.select("value_df1", "value_df2", "account_id", "tokenization_type")
df_final.show()
