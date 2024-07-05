
from pyspark.sql import SparkSession
from pyspark.sql.functions import length

# Initialize Spark session
spark = SparkSession.builder.appName("AddLengthColumn").getOrCreate()

# Assuming df is your existing DataFrame
# Add a new column 'value_length' which is the length of the 'value' column
df = df.withColumn('value_length', length(df['value']))

# Show the DataFrame with the new column
df.show()
