
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

# Join df1 with df2 on account_id, attribute, and tokenization_type
joined_df = df1.alias("df1").join(df2.alias("df2"), 
                                  (col("df1.account_id") == col("df2.account_id")) & 
                                  (col("df1.attribute") == col("df2.attribute")) &
                                  (col("df1.tokenization_type") == col("df2.tokenization_type")), 
                                  how="left")

# Update df1 value column based on the conditions
updated_df1 = joined_df.withColumn(
    "value",
    when((col("df1.attribute") == "Social security number") & (col("df1.tokenization_type") == "USTAXID"), col("df2.value"))
    .when((col("df1.attribute") == "Consumer Account Number") & (col("df1.tokenization_type") == "PAN"), col("df2.value"))
    .otherwise(col("df1.value"))
).select(col("df1.account_id"), col("df1.attribute"), col("value"), col("df1.tokenization_type"))

# Show the updated dataframe
updated_df1.show()

# Save the updated dataframe if needed
updated_df1.write.csv("/mnt/data/updated_df1.csv")
