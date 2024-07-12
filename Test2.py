

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import random

# Initialize Spark session
spark = SparkSession.builder.appName("CombineDataExample").getOrCreate()

# Define schema
schema = StructType([
    StructField("account_id", StringType(), nullable=True),
    StructField("attribute", StringType(), nullable=True),
    StructField("value", StringType(), nullable=True),
    StructField("tokenization_type", StringType(), nullable=True)
])

# Function to generate unique account_id and value
def generate_unique_data(num_records):
    account_ids = set()
    values = set()
    data = []
    
    while len(account_ids) < num_records or len(values) < num_records:
        account_id = str(random.randint(10**16, 10**17 - 1))
        attribute = random.choice(["Social Security Number", "Consumer Account Number"])
        
        if attribute == "Social Security Number":
            value = str(random.randint(10**8, 10**9 - 1))
            tokenization_type = "USTAXID"
        else:  # Consumer Account Number
            value = account_id
            tokenization_type = "PAN"
        
        if account_id not in account_ids and value not in values:
            account_ids.add(account_id)
            values.add(value)
            data.append((account_id, attribute, value, tokenization_type))
    
    return data

# Generate data
num_records = 100  # Specify the number of records you want to generate
generated_data = generate_unique_data(num_records)

# Create DataFrame
df = spark.createDataFrame(generated_data, schema)

# Show DataFrame
df.show()
