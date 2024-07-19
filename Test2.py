
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrame Join Example").getOrCreate()

# Sample data for df1
data1 = [
    ("1000000000323351", "USTAXID", "323353"),
    ("1000000000323351", "PAN", "323353"),
    ("1000000000321616", "PAN", "321616"),
    ("1000000000321616", "USTAXID", "321618")
]

columns1 = ["account_number", "tokenization", "output_record_sequence"]

df1 = spark.createDataFrame(data1, columns1)

# Sample data for df2
data2 = [
    ("66089756365870836", "Social Security Number", "KxWQmIGGm", "USTAXID"),
    ("4947456522358753", "Social Security Number", "KxK7DWG0V", "USTAXID"),
    ("8801333eQCia23421", "Consumer Account Number", "8801333zeQCia23421", "PAN"),
    ("5913597ecT8JA3895", "Consumer Account Number", "5913593ecT8JA3895", "PAN")
]

columns2 = ["account_number", "attribute", "formatted", "tokenization"]

df2 = spark.createDataFrame(data2, columns2)

# Perform the join
joined_df = df2.join(df1, on="tokenization")

# Filter to ensure each tokenization type has different account numbers
filtered_df = joined_df.filter(
    (col("tokenization") == "USTAXID") & (col("output_record_sequence") == "321618") |
    (col("tokenization") == "USTAXID") & (col("output_record_sequence") == "323353") |
    (col("tokenization") == "PAN") & (col("output_record_sequence") == "321616") |
    (col("tokenization") == "PAN") & (col("output_record_sequence") == "323353")
)

# Select and rename columns to match the desired output
result_df = filtered_df.select(
    col("df1.account_number").alias("account_number"),
    col("attribute"),
    col("formatted"),
    col("tokenization")
)

# Show the result
result_df.show(truncate=False)
