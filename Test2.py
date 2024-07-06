
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define a UDF to convert the string to its Unicode code points
def to_unicode_repr(value):
    if value is None:
        return None
    return ''.join([f'\\u{ord(char):04x}' for char in value])

unicode_repr_udf = udf(to_unicode_repr, StringType())

# Add a new column with the Unicode representation of the value
df = df.withColumn('value_unicode', unicode_repr_udf(df['value']))

# Show the DataFrame with the new column to inspect hidden characters
df.select('value', 'value_unicode', 'value_length').show(truncate=False)
