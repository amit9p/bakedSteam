

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Start a Spark session
spark = SparkSession.builder.appName("Update Values").getOrCreate()

# Manually define the new values data from the output image
data = [
    ("45222355101215271", "xZo9Hwvxt"),
    ("45263507888435271", "3EmFHfJ9y"),
    ("92842214958721143", "JQSRHZyjz"),
    ("85045324729033015", "jIAEVD1YL"),
    ("65119352382665432", "2EiCecUKd"),
    ("94130412571123703", "xM90suRIM"),
    ("83213355553618025", "b8OAzy78s"),
    ("94130595791703703", "q8qe3dOHX"),
    ("92842351896971143", "2XaGMJ9h0"),
    ("65119367539575432", "YPe8s9H63"),
    ("92842398982541143", "HAlRraLH3"),
    ("60331430580262428", "H4E6SDXLF"),
    ("60331365887472428", "YIyGQNvsJ"),
    ("74318433636492202", "9Mw0yI8Xz")
]

# Define the schema for the new values DataFrame
schema = "account_id STRING, value STRING"

# Create a DataFrame with the new values
df_new_values = spark.createDataFrame(data, schema=schema)

# Load your main DataFrame (df_main)
# Assuming df_main is loaded with the schema shown in your image
# Example: df_main = spark.read.parquet("path_to_main_dataframe")

# Join the DataFrames on account_id
df_joined = df_main.join(df_new_values, "account_id", "left")

# Update the value column only where attribute is 'social security number'
df_updated = df_joined.withColumn("value", when(col("attribute") == "social security number", col("df_new_values.value")).otherwise(col("df_main.value")))

# Drop any redundant or unwanted columns, if necessary
df_final = df_updated.drop(df_new_values.value)

# Show the updated DataFrame
df_final.show()
