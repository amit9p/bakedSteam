

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import when, lit, concat

# Initialize Spark session
spark = SparkSession.builder.appName("CreateDataFrameFromImages").getOrCreate()

# Data extracted from the images
data = [
    {"account_id": "60331430580262428", "run_id": "dfd8c4e9-4db7-4b12-8e3e-d5dc4dbf8c22", "segment": "BASE", "attribute": "Record Descriptor Word (RDW)", "value": "0626", "row_position": 5, "column_position": 1, "file_type": "metro2-all", "business_date": "2024-03-24", "__index_level_0__": 219, "tokenization_type": None},
    {"account_id": "60331430580262428", "run_id": "dfd8c4e9-4db7-4b12-8e3e-d5dc4dbf8c22", "segment": "BASE", "attribute": "Processing Indicator", "value": "1", "row_position": 5, "column_position": 2, "file_type": "metro2-all", "business_date": "2024-03-24", "__index_level_0__": 220, "tokenization_type": None},
    {"account_id": "60331430580262428", "run_id": "dfd8c4e9-4db7-4b12-8e3e-d5dc4dbf8c22", "segment": "BASE", "attribute": "Consumer Account Number", "value": "60331430580262428", "row_position": 5, "column_position": 3, "file_type": "metro2-all", "business_date": "2024-03-24", "__index_level_0__": 221, "tokenization_type": "PAN"},
    {"account_id": "60331430580262428", "run_id": "dfd8c4e9-4db7-4b12-8e3e-d5dc4dbf8c22", "segment": "BASE", "attribute": "Social Security Number", "value": "H4E6SDXLF", "row_position": 5, "column_position": 4, "file_type": "metro2-all", "business_date": "2024-03-24", "__index_level_0__": 222, "tokenization_type": "USTAXID"},
    {"account_id": "60331430580262428", "run_id": "dfd8c4e9-4db7-4b12-8e3e-d5dc4dbf8c22", "segment": "BASE", "attribute": "Telephone Number", "value": "0000000000", "row_position": 5, "column_position": 5, "file_type": "metro2-all", "business_date": "2024-03-24", "__index_level_0__": 223, "tokenization_type": None}
]

# Convert the list of dictionaries to a PySpark DataFrame
df = spark.createDataFrame([Row(**i) for i in data])

# Modify the `value` column for the "Consumer Account Number"
df = df.withColumn("value", when(df["attribute"] == "Consumer Account Number", concat(df["value"], lit(" " * 13))).otherwise(df["value"]))

# Show the modified DataFrame
df.show(truncate=False)
