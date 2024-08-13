

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, udf
from pyspark.sql.types import StringType

# Start Spark session
spark = SparkSession.builder.appName("Format Adjustment").getOrCreate()

# Define the Python function to append spaces
def append_spaces(formatted, spaces_to_append):
    return formatted + (" " * max(0, 30 - spaces_to_append))

# Register the function as a UDF
append_spaces_udf = udf(append_spaces, StringType())

# Load your DataFrame (assuming df is your DataFrame)
# Example to load DataFrame:
# df = spark.read.csv("path_to_your_file.csv", header=True, inferSchema=True)

# Calculate current length and necessary spaces to append
df = df.withColumn('current_length', length(col('formatted')))
df = df.withColumn('spaces_to_append', 30 - col('current_length'))

# Apply the UDF
df = df.withColumn('formatted', 
                   when(col('tokenization') == 'PAN', 
                        append_spaces_udf(col('formatted'), col('spaces_to_append')))
                   .otherwise(col('formatted')))

# Show the updated DataFrame
df.show()
df.printSchema()
