

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, udf, when
from pyspark.sql.types import StringType

# Start Spark session
spark = SparkSession.builder.appName("Format Adjustment").getOrCreate()

# Define the Python function to append spaces
def append_spaces(formatted, spaces_to_append):
    return formatted + (" " * max(0, spaces_to_append))

# Register the function as a UDF
append_spaces_udf = udf(append_spaces, StringType())

# Assume your DataFrame is loaded as df
# df = spark.read...

# Calculate current length and spaces to append
df = df.withColumn('current_length', length(col('formatted')))
df = df.withColumn('spaces_to_append', 30 - col('current_length'))

# Use the UDF in a DataFrame API
df = df.withColumn(
    'formatted',
    when(col('tokenization') == 'PAN', append_spaces_udf(col('formatted'), col('spaces_to_append')))
    .otherwise(col('formatted'))
)

# Show the result and check the DataFrame schema to ensure everything is correct
df.show()
df.printSchema()
