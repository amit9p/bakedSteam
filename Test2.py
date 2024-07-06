

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import when, lit, concat
import random

# Initialize Spark session
spark = SparkSession.builder.appName("GenerateMultipleAccountRecords").getOrCreate()

# Define the function to generate a 17-digit random number
def generate_17_digit_number():
    return str(random.randint(10**16, 10**17 - 1))

# Define the template data
template_data = [
    {"attribute": "Record Descriptor Word (RDW)", "value": "0626", "row_position": 5, "column_position": 1, "file_type": "metro2-all", "business_date": "2024-03-24", "__index_level_0__": 219, "tokenization_type": None},
    {"attribute": "Processing Indicator", "value": "1", "row_position": 5, "column_position": 2, "file_type": "metro2-all", "business_date": "2024-03-24", "__index_level_0__": 220, "tokenization_type": None},
    {"attribute": "Consumer Account Number", "value": "60331430580262428", "row_position": 5, "column_position": 3, "file_type": "metro2-all", "business_date": "2024-03-24", "__index_level_0__": 221, "tokenization_type": "PAN"},
    {"attribute": "Social Security Number", "value": "H4E6SDXLF", "row_position": 5, "column_position": 4, "file_type": "metro2-all", "business_date": "2024-03-24", "__index_level_0__": 222, "tokenization_type": "USTAXID"},
    {"attribute": "Telephone Number", "value": "0000000000", "row_position": 5, "column_position": 5, "file_type": "metro2-all", "business_date": "2024-03-24", "__index_level_0__": 223, "tokenization_type": None}
]

# Number of account IDs to generate
num_accounts = 10

# Initialize a list to hold all records
all_data = []

for _ in range(num_accounts):
    account_id = generate_17_digit_number()
    run_id = "dfd8c4e9-4db7-4b12-8e3e-d5dc4dbf8c22"  # Keeping run_id same for simplicity
    segment = "BASE"

    for record in template_data:
        # Copy the template record and update the account_id and value if needed
        new_record = record.copy()
        new_record["account_id"] = account_id
        new_record["run_id"] = run_id
        new_record["segment"] = segment
        if new_record["attribute"] == "Consumer Account Number":
            new_record["value"] = account_id + " " * 13
        
        all_data.append(Row(**new_record))

# Convert the list of dictionaries to a PySpark DataFrame
df = spark.createDataFrame(all_data)

# Show the DataFrame
df.show(truncate=False)
