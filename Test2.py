

from pyspark.sql import SparkSession
from pyspark.sql.functions import length

# Initialize Spark session
spark = SparkSession.builder.appName("AddLengthColumn").getOrCreate()

# Load your DataFrame (assuming you have loaded it already)
# df = spark.read.parquet("/path/to/your/data.parquet")  # Example of loading a DataFrame

# Add a new column 'value_length' which is the length of the 'value' column
df = df.withColumn('value_length', length(df['value']))

# Show the DataFrame with the new column
df.show()
