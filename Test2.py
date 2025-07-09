
from pyspark.sql.types import StructType, StructField, StringType

# 1) Define the schema, marking delinquency_status as non-nullable
forced_schema = StructType([
    StructField("account_id",          StringType(), True),   # still nullable if you want
    StructField("delinquency_status",  StringType(), False)   # now non-nullable
])

# 2) Recreate your expected_df with that schema
expected_df_nonnull = spark.createDataFrame(
    expected_df.rdd,      # your original RDD of rows
    schema=forced_schema  # the schema you want
)

# 3) Verify
expected_df_nonnull.printSchema()
# root
#  |-- account_id: string (nullable = true)
#  |-- delinquency_status: string (nullable = false)
