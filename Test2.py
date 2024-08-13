
from pyspark.sql.functions import col, length, when, udf
from pyspark.sql.types import StringType

# Define and register the UDF
def append_spaces(formatted, spaces):
    return formatted + (" " * spaces)

append_spaces_udf = udf(append_spaces, StringType())

# Assuming df is your DataFrame
df = df.withColumn('current_length', length(col('formatted')))
df = df.withColumn('spaces_to_append', 30 - col('current_length'))

# Apply the UDF
df = df.withColumn('formatted', when(col('tokenization') == 'PAN', append_spaces_udf(col('formatted'), col('spaces_to_append'))).otherwise(col('formatted')))

# Drop the extra columns if they are no longer needed
df = df.drop('current_length', 'spaces_to_append')

# Continue with your processing, now df can be unioned with other DataFrames that have the same original schema
