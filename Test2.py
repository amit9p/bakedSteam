
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# Initialize Spark session
spark = SparkSession.builder.appName("CombineDataExample").getOrCreate()

# Sample data from both images (excluding value_length column)
data_combined = [
    (None, "dfd8c4e9-4db7-4b1c-a72b-4aac035a5dae", "HEADER", "Record Descriptor Word (RDW)", "0426", 1, 1, "metro2-all", "2024-03-24", 0, None),
    (None, "dfd8c4e9-4db7-4b1c-a72b-4aac035a5dae", "HEADER", "TransUnion Program Identifier", "COF09RECOV", 1, 1, "metro2-all", "2024-03-24", 1, None),
    (None, "dfd8c4e9-4db7-4b1c-a72b-4aac035a5dae", "HEADER", "Innovis Program Identifier", "COF09RECOC", 1, 1, "metro2-transunion", "2024-03-24", 2, None),
    (None, "dfd8c4e9-4db7-4b1c-a72b-4aac035a5dae", "HEADER", "Equifax Program Identifier", "COF09RECOB", 1, 1, "metro2-transunion", "2024-03-24", 3, None),
    (None, "dfd8c4e9-4db7-4b1c-a72b-4aac035a5dae", "HEADER", "Experian Program Identifier", "COF09RECOA", 1, 1, "metro2-transunion", "2024-03-24", 4, None),
    (None, "dfd8c4e9-4db7-4b1c-a72b-4aac035a5dae", "TRAILER", "Record Descriptor Word (RDW)", "0426", 1, 1, "metro2-all", "2024-03-24", 5, None)
]

# Define schema excluding value_length
schema = ["account_id", "run_id", "segment", "attribute", "value", "row_position", "column_position", "file_type", "business_date", "index_level_0", "tokenization_type"]

# Create DataFrame
df = spark.createDataFrame(data_combined, schema)

# Modify the value column based on the attribute
df = df.withColumn(
    "value",
    when(col("attribute") == "Innovis Program Identifier", lit(" " * 10))
    .when(col("attribute") == "Equifax Program Identifier", lit(" " * 10))
    .when(col("attribute") == "Experian Program Identifier", lit(" " * 5))
    .otherwise(col("value"))
)

# Show the resulting DataFrame
df.show(truncate=False)
