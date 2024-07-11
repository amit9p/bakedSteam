
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


####

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("CountAttributes").getOrCreate()

# Sample DataFrame creation (replace this with loading your DataFrame)
data = [
    ("1", "23545616256127562", "segment1", "Social security number", "value1", 1, 1, "file_type1", "2023-07-10"),
    ("2", "23545616256127563", "segment2", "Consumer Account Number", "value2", 2, 2, "file_type2", "2023-07-11"),
    ("3", "23545616256127564", "segment3", "Social security number", "value3", 3, 3, "file_type3", "2023-07-12"),
    # Add more records as needed
]

schema = ["run_id", "account_id", "segment", "attribute", "value", "row_position", "column_position", "file_type", "business_date"]

df = spark.createDataFrame(data, schema)

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("attributes_table")

# SQL query to count the occurrences of each attribute
query = """
SELECT 
    attribute,
    COUNT(*) as count
FROM 
    attributes_table
WHERE 
    attribute IN ('Social security number', 'Consumer Account Number')
GROUP BY 
    attribute
"""

# Execute the query
result_df = spark.sql(query)

# Show the result
result_df.show()

# Stop the Spark session
spark.stop()

