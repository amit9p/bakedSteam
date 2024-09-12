
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

# Define the schema
schema = StructType([
    StructField("business_date", StringType(), True),
    StructField("run_identifier", LongType(), True),
    StructField("output_filetype", ArrayType(StructType([
        StructField("element", StringType(), True)
    ])), True),
    StructField("output_record_sequence", LongType(), True),
    StructField("output_field_sequence", LongType(), True),
    StructField("attribute", StringType(), True),
    StructField("formatted", StringType(), True),
    StructField("tokenization", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("segment", StringType(), True)
])

# Read the Parquet file with the specified schema
df = spark.read.format("parquet").schema(schema).load("/mnt/data/file-bCmwHuDLwEIwcieA2BfGGsZL")

# Show the DataFrame schema and content
df.printSchema()
df.show(truncate=False)
