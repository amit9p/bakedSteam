
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, udf, expr
from pyspark.sql.types import StringType

# Start Spark session
spark = SparkSession.builder.appName("Format Adjustment").getOrCreate()

# Define the Python function to append spaces
def append_spaces(formatted, spaces_to_append):
    if spaces_to_append > 0:
        return formatted + (" " * spaces_to_append)
    else:
        return formatted

# Register the function as a UDF
append_spaces_udf = udf(append_spaces, StringType())

# Assume your DataFrame is loaded as df
# df = spark.read...

# Calculate current length and spaces to append
df = df.withColumn('current_length', length(col('formatted')))
df = df.withColumn('spaces_to_append', expr("30 - current_length"))

# Use the UDF in an expression with a CASE statement
df = df.withColumn(
    'formatted', 
    expr("CASE WHEN tokenization = 'PAN' THEN append_spaces_udf(formatted, spaces_to_append) ELSE formatted END")
)

# Show the result and check the DataFrame schema to ensure everything is correct
df.show()
df.printSchema()
