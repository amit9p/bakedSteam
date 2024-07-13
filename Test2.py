
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameUpdate").getOrCreate()

# Define schema for the dataframes
schema_df1 = """account_id STRING, attribute STRING, value STRING, tokenization_type STRING"""
schema_df2 = """run_id STRING, account_id STRING, segment STRING, attribute STRING, value STRING, row_position LONG, column_position LONG, file_type STRING, business_date STRING, tokenization_type STRING"""

# Load dataframes (assuming they are CSV files; adjust the code if the format is different)
df1 = spark.read.schema(schema_df1).csv("/mnt/data/file-Wnfpw5geRk26VAZpmLXxYmrI")
df2 = spark.read.schema(schema_df2).csv("/mnt/data/file-L6fn1TawVLOfIn1XCg8HWB6f")

# Filter df2 based on the required conditions
df2_ssn = df2.filter((col("attribute") == "Social security number") & (col("tokenization_type") == "USTAXID"))
df2_can = df2.filter((col("attribute") == "Consumer Account Number") & (col("tokenization_type") == "PAN"))

# Join df1 with filtered df2 dataframes
df1_joined = df1 \
    .join(df2_ssn.select("value", "run_id", "segment", "row_position", "column_position", "file_type", "business_date").withColumnRenamed("value", "ssn_value"), on=[df1.attribute == "Social security number", df1.tokenization_type == "USTAXID"], how="left") \
    .join(df2_can.select("value", "run_id", "segment", "row_position", "column_position", "file_type", "business_date").withColumnRenamed("value", "can_value"), on=[df1.attribute == "Consumer Account Number", df1.tokenization_type == "PAN"], how="left")

# Update df1 value column based on the conditions
df1_updated = df1_joined.withColumn(
    "value",
    when((col("attribute") == "Social security number") & (col("tokenization_type") == "USTAXID"), col("ssn_value"))
    .when((col("attribute") == "Consumer Account Number") & (col("tokenization_type") == "PAN"), col("can_value"))
    .otherwise(col("value"))
).select(col("df1.account_id"), col("df1.attribute"), col("value"), col("df1.tokenization_type"),
         col("run_id"), col("segment"), col("row_position"), col("column_position"), col("file_type"), col("business_date"))

# Show the updated dataframe
df1_updated.show()

# Save the updated dataframe if needed
df1_updated.write.csv("/mnt/data/updated_df1.csv")
