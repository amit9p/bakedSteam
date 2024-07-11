
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Create a Spark session
spark = SparkSession.builder.appName("AddColumnExample").getOrCreate()

# Sample DataFrame creation (replace this with loading your DataFrame)
data = [
    ("1", "23545616256127562", "segment1", "Social security number", "value1", 1, 1, "file_type1", "2023-07-10"),
    ("2", "23545616256127563", "segment2", "Consumer Account Number", "value2", 2, 2, "file_type2", "2023-07-11"),
    ("3", "23545616256127564", "segment3", "Other", "value3", 3, 3, "file_type3", "2023-07-12"),
    # Add more records as needed
]

schema = ["run_id", "account_id", "segment", "attribute", "value", "row_position", "column_position", "file_type", "business_date"]

df = spark.createDataFrame(data, schema)

# Adding tokenization_type column
df = df.withColumn(
    "tokenization_type",
    when(col("attribute") == "Social security number", "USTAXID")
    .when(col("attribute") == "Consumer Account Number", "PAN")
    .otherwise(None)
)

# Show the updated DataFrame
df.show()

# Stop the Spark session
spark.stop()
