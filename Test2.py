

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType

spark = SparkSession.builder.getOrCreate()

# Your mapping data
mapping_data = [
    (4146147380, 1001315206, 7777771001),
    (4146146886, 1004043965, 7777771002),
    (3477618751, 1004043969, 7777771003),
    (3477618760, 1004043979, 7777771004),
    (4454193610, 1004043992, 7777771005),
    (4454193717, 1004043993, 7777771006),
    (3477618797, 1004044027, 7777771007),
    (3477618801, 1004044038, 7777771008),
    (4454193181, 1004044027, 7777771009),
    (3477618821, 1004044038, 7777771010)
]

# Convert to RDD
rdd = spark.sparkContext.parallelize(mapping_data)

# Define schema
schema = StructType([
    StructField("customer_pk_id", LongType(), True),
    StructField("new_customer_id", LongType(), True),
    StructField("new_account_id", LongType(), True)
])

# Create DataFrame from RDD with schema
mapping_df = spark.createDataFrame(rdd, schema)

# Show result
mapping_df.show()
