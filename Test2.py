
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameMerge").getOrCreate()

# Load the dataframes
df1 = spark.read.option("header", "true").csv("/mnt/data/file-C616nK97kHWBl68CqpD9VFt4")
df2 = spark.read.option("header", "true").csv("/mnt/data/file-n4ljITUBPe0nYbp1eF0NmRkG")

# Show the schemas for verification
df1.printSchema()
df2.printSchema()

# Add a unique id to df1 to ensure we can join back uniquely
df1 = df1.withColumn("unique_id", monotonically_increasing_id())

# Create separate dataframes for each tokenization type
df1_ustaxid = df1.filter(col("tokenization_type") == "ustaxid")
df1_pan = df1.filter(col("tokenization_type") == "pan")

# Rename columns in df2 to avoid conflicts during the join
df2_ustaxid = df2.withColumnRenamed("Social Security Number", "ssn_value").withColumnRenamed("account_id", "account_id_ssn")
df2_pan = df2.withColumnRenamed("Consumer Account Number", "pan_value").withColumnRenamed("account_id", "account_id_pan")

# Join df1 with df2 based on the tokenization type
df1_ustaxid = df1_ustaxid.join(df2_ustaxid, df1_ustaxid.account_id == df2_ustaxid.account_id_ssn, "left").drop("account_id_ssn")
df1_pan = df1_pan.join(df2_pan, df1_pan.account_id == df2_pan.account_id_pan, "left").drop("account_id_pan")

# Replace the value column with the corresponding values from df2
df1_ustaxid = df1_ustaxid.withColumn("value", col("ssn_value")).drop("ssn_value")
df1_pan = df1_pan.withColumn("value", col("pan_value")).drop("pan_value")

# Combine the dataframes back together
df1_combined = df1_ustaxid.union(df1_pan)

# Show the resulting dataframe
df1_combined.show()
