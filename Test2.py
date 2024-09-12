
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

# Define the schema
schema = StructType([
    StructField("business_date", StringType(), True),
    StructField("run_identifier", LongType(), True),
    StructField("output_filetype", ArrayType(StringType(), True), True),
    StructField("output_record_sequence", LongType(), True),
    StructField("output_field_sequence", LongType(), True),
    StructField("attribute", StringType(), True),
    StructField("formatted", StringType(), True),
    StructField("tokenization", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("segment", StringType(), True)
])

# Apply the schema to your DataFrame (assuming you're reading a file)
df = spark.read.format("your_format").schema(schema).load("path_to_your_data")

# Show the DataFrame to verify schema application
df.printSchema()
df.show()
