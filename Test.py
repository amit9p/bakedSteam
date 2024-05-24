# Create the data
data = [("zXo9Hwxt",), ("value2",), ("value3",)]

# Create the DataFrame
final_df = spark.createDataFrame(data, schema)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Create DataFrame Example") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("value", StringType(), True),
    StructField("account_id", StringType(), True)
])

# Create the data
data = [("z3P9Y75KL", "60331365887472428")]

# Create the DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()
