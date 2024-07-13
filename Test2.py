

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameReplacement").getOrCreate()

# Load the dataframes
df1 = spark.read.parquet("/mnt/data/file-U4qkPznEyieGeGhGQns07nx7")
df2 = spark.read.parquet("/mnt/data/file-C5CTELwnzCKFOajIoqYsj7rk")

# Filter and create mappings from df1
df1_ssn = df1.filter((df1["attribute"] == "Social Security Number") & (df1["tokenization_type"] == "USTAXID")) \
             .select("attribute", "tokenization_type", "value").rdd \
             .map(lambda row: ((row["attribute"], row["tokenization_type"]), row["value"])) \
             .collectAsMap()

df1_pan = df1.filter((df1["attribute"] == "Consumer Account Number") & (df1["tokenization_type"] == "PAN")) \
             .select("attribute", "tokenization_type", "value").rdd \
             .map(lambda row: ((row["attribute"], row["tokenization_type"]), row["value"])) \
             .collectAsMap()

# Broadcast the mappings
ssn_mapping_broadcast = spark.sparkContext.broadcast(df1_ssn)
pan_mapping_broadcast = spark.sparkContext.broadcast(df1_pan)

# Define a function to replace the values
def replace_values(attribute, tokenization_type, value):
    if attribute == "Social Security Number" and tokenization_type == "USTAXID":
        return ssn_mapping_broadcast.value.get((attribute, tokenization_type), value)
    elif attribute == "Consumer Account Number" and tokenization_type == "PAN":
        return pan_mapping_broadcast.value.get((attribute, tokenization_type), value)
    else:
        return value

# Register the function as a UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

replace_values_udf = udf(replace_values, StringType())

# Apply the UDF to replace the values
df2_final = df2.withColumn("value", replace_values_udf(col("attribute"), col("tokenization_type"), col("value")))

# Show the updated dataframe
df2_final.show()
