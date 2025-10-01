from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DateType, StringType, DoubleType

schema = StructType([
    StructField("account_id", LongType(), True),   # Long, not String
    StructField("recap_sequence", IntegerType(), True),
    StructField("transaction_posting_date", DateType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("transaction_category", StringType(), True),
    StructField("transaction_source", StringType(), True),
    StructField("transaction_description", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_resulting_balance", DoubleType(), True)
])






from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType

spark = SparkSession.builder.getOrCreate()

# Schema based on your image
schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("recap_sequence", IntegerType(), True),
    StructField("transaction_posting_date", DateType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("transaction_category", StringType(), True),
    StructField("transaction_source", StringType(), True),
    StructField("transaction_description", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_resulting_balance", DoubleType(), True)
])

# Sample rows (copied from your screenshot)
data = [
    ("1713558881", 133, "2020-02-26", "2020-02-26", "PAYMENT", "IEPS", "175292049842333844342", -150.0, 6555.28),
    ("1713558881", 134, "2020-02-26", "2020-02-26", "PAYMENT", "IEPS", "175292049842333844342", -150.0, 6405.28),
    ("1713558881", 135, "2020-02-26", "2020-02-26", "PAYMENT", "IEPS", "175292049842333844342", -150.0, 6255.28),
    ("1713558881", 136, "2020-02-26", "2020-02-26", "PAYMENT", "IEPS", "175292049842333844342", -150.0, 6105.28),
    ("1713558881", 137, "2020-02-26", "2020-02-26", "PAYMENT", "IEPS", "175292049842333844342", -150.0, 5955.28),
    ("1713558881", 138, "2020-02-26", "2020-02-26", "PAYMENT", "IEPS", "175292049842333844342", -150.0, 5805.28),
    ("1713558881", 139, "2020-02-26", "2020-02-26", "PAYMENT", "IEPS", "175292049842333844342", -150.0, 5655.28),
    ("1713558881", 140, "2020-02-26", "2020-02-26", "PAYMENT", "IEPS", "175292049842333844342", -150.0, 5505.28),
    ("1713558881", 141, "2020-02-26", "2020-02-26", "PAYMENT", "IEPS", "175292049842333844342", -150.0, 5355.28),
    ("1713558881", 142, "2020-02-26", "2020-02-26", "PAYMENT", "IEPS", "175292049842333844342", -150.0, 5205.28)
]

df = spark.createDataFrame(data, schema=schema)

df.show(truncate=False)




