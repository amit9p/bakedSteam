
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, lit, expr

# Start Spark session
spark = SparkSession.builder.appName("Format Adjustment").getOrCreate()

# Load your DataFrame (assuming df is your DataFrame)
# For example: df = spark.read.csv("path_to_your_file.csv", inferSchema=True, header=True)

# Calculate the number of spaces to append
df = df.withColumn('current_length', length(col('formatted')))
df = df.withColumn('spaces_to_append', expr("30 - current_length"))

# Function to append spaces where tokenization is 'PAN'
def append_spaces(formatted, spaces_to_append):
    if spaces_to_append > 0:
        return formatted + (" " * spaces_to_append)
    else:
        return formatted

# Register the function as a UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
append_spaces_udf = udf(append_spaces, StringType())

# Apply the UDF to the DataFrame
df = df.withColumn('formatted', expr("CASE WHEN tokenization = 'PAN' THEN append_spaces_udf(formatted, spaces_to_append) ELSE formatted END"))

# Show the result
df.show()
