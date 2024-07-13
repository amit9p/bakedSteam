
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameUpdate").getOrCreate()

# Define schema for the dataframes
schema_df1 = """account_id STRING, attribute STRING, value STRING, tokenization_type STRING"""
schema_df2 = """run_id STRING, account_id STRING, segment STRING, attribute STRING, value STRING, row_position LONG, column_position LONG, file_type STRING, business_date STRING, tokenization_type STRING"""

# Load dataframes (assuming they are CSV files; adjust the code if the format is different)
df1 = spark.read.schema(schema_df1).csv("/path/to/df1.csv")
df2 = spark.read.schema(schema_df2).csv("/path/to/df2.csv")

# Define conditions for replacement
condition1 = (df1.attribute == "Social security number") & (df1.tokenization_type == "USTAXID")
condition2 = (df1.attribute == "Consumer Account Number") & (df1.tokenization_type == "PAN")

# Perform the replacements
df1_updated = df1.withColumn(
    "value",
    when(condition1, df2.filter((df2.attribute == "Social security number") & (df2.tokenization_type == "USTAXID")).select("value").first()[0])
    .when(condition2, df2.filter((df2.attribute == "Consumer Account Number") & (df2.tokenization_type == "PAN")).select("value").first()[0])
    .otherwise(col("value"))
)

# Show the updated dataframe
df1_updated.show()

# Save the updated dataframe if needed
df1_updated.write.csv("/path/to/updated_df1.csv")
