
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import random

# Create a Spark session
spark = SparkSession.builder.appName("UniqueDataFrame").getOrCreate()

# Define schema
schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("value", StringType(), True)
])

# Function to generate unique account_id and value
def generate_unique_data(num_records):
    account_ids = set()
    values = set()
    data = []

    while len(account_ids) < num_records or len(values) < num_records:
        account_id = str(random.randint(10**16, 10**17 - 1))
        value = str(random.randint(10**8, 10**9 - 1))

        if account_id not in account_ids and value not in values:
            account_ids.add(account_id)
            values.add(value)
            data.append((account_id, value))
    
    return data

# Generate 10 unique records
data = generate_unique_data(10)

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()

# Stop Spark session
spark.stop()
