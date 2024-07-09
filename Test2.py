
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
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
schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("attribute", StringType(), True),
    StructField("value", StringType(), True),
    StructField("row_position", IntegerType(), True),
    StructField("column_position", IntegerType(), True),
    StructField("file_type", StringType(), True),
    StructField("business_date", StringType(), True),
    StructField("index_level_0", IntegerType(), True),
    StructField("tokenization_type", StringType(), True)
])

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
