

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()

# Sample data from the images
data1 = [
    (None, "dfd8c4e9-4db7-4b1c-a72b-4aac035a5dae", "HEADER", "Record Descriptor Word (RDW)", "0426", 1, 1),
    (None, "dfd8c4e9-4db7-4b1c-a72b-4aac035a5dae", "HEADER", "TransUnion Program Identifier", "COF09RECOV", 1, 1),
    (None, "dfd8c4e9-4db7-4b1c-a72b-4aac035a5dae", "HEADER", "Innovis Program Identifier", "COF09RECOC", 1, 1),
    (None, "dfd8c4e9-4db7-4b1c-a72b-4aac035a5dae", "HEADER", "Equifax Program Identifier", "COF09RECOB", 1, 1),
    (None, "dfd8c4e9-4db7-4b1c-a72b-4aac035a5dae", "HEADER", "Experian Program Identifier", "COF09RECOA", 1, 1),
    (None, "dfd8c4e9-4db7-4b1c-a72b-4aac035a5dae", "TRAILER", "Record Descriptor Word (RDW)", "0426", 1, 1)
]

data2 = [
    ("0426", 1, 1, "metro2-all", "2024-03-24", 0, None, 4),
    ("COF09RECOV", 1, 1, "metro2-all", "2024-03-24", 1, None, 10),
    ("COF09RECOC", 1, 1, "metro2-transunion", "2024-03-24", 2, None, 10),
    ("COF09RECOB", 1, 1, "metro2-transunion", "2024-03-24", 3, None, 10),
    ("COF09RECOA", 1, 1, "metro2-transunion", "2024-03-24", 4, None, 5),
    ("0426", 1, 1, "metro2-all", "2024-03-24", 5, None, 4)
]

# Define schema
schema1 = ["account_id", "run_id", "segment", "attribute", "value", "row_position", "column_position"]
schema2 = ["value", "row_position", "column_position", "file_type", "business_date", "index_level_0", "tokenization_type", "value_length"]

# Create DataFrames
df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)

# Modify the value column based on the attribute
df1 = df1.withColumn(
    "value",
    when(col("attribute") == "Innovis Program Identifier", col("value").substr(1, 0) + "          ")
    .when(col("attribute") == "Equifax Program Identifier", col("value").substr(1, 0) + "          ")
    .when(col("attribute") == "Experian Program Identifier", col("value").substr(1, 0) + "     ")
    .otherwise(col("value"))
)

# Show the resulting DataFrame
df1.show(truncate=False)
df2.show(truncate=False)
