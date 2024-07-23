
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CheckAccountNumbers").getOrCreate()

# Load the data from Parquet file
df = spark.read.parquet("/mnt/data/file-aHP3wJIRFIZ1CbFDmkr5lUgv")

# Create a temporary view
df.createOrReplaceTempView("data")

# SQL query to find output_record_sequence with different account_number values
query = """
SELECT output_record_sequence
FROM (
    SELECT output_record_sequence, account_number,
           COUNT(DISTINCT account_number) AS account_count
    FROM data
    GROUP BY output_record_sequence, account_number
) subquery
GROUP BY output_record_sequence
HAVING COUNT(*) > 1
"""

# Execute the query
result = spark.sql(query)

# Show the result
result.show()
