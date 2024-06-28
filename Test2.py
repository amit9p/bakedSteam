
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Create DataFrame Example") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("value", StringType(), True)
])

# Create the data
data = [
    ("45222355101215271", "JEiIxYTqx"),
    ("45263507888435271", "hhIbo28dB")
]

# Create the DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()
