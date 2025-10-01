


import datetime
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DateType, StringType, DoubleType

schema = StructType([
    StructField("account_id", LongType(), True),
    StructField("recap_sequence", IntegerType(), True),
    StructField("transaction_posting_date", DateType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("transaction_category", StringType(), True),
    StructField("transaction_source", StringType(), True),
    StructField("transaction_description", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_resulting_balance", DoubleType(), True)
])

# ✅ Use datetime.date for DateType fields
data = [
    (7777771001, 1, datetime.date(2020, 2, 26), datetime.date(2020, 2, 26), "PAYMENT", "IEPS", "175292049842333844342", -150.0, 6555.28),
    (7777771001, 2, datetime.date(2020, 2, 26), datetime.date(2020, 2, 26), "PAYMENT", "IEPS", "175292049842333844342", -150.0, 6405.28),
    (7777771001, 3, datetime.date(2020, 2, 26), datetime.date(2020, 2, 26), "PAYMENT", "IEPS", "175292049842333844342", -150.0, 6255.28),
    (7777771001, 4, datetime.date(2020, 2, 26), datetime.date(2020, 2, 26), "PAYMENT", "IEPS", "175292049842333844342", -150.0, 6105.28),
    (7777771001, 5, datetime.date(2020, 2, 26), datetime.date(2020, 2, 26), "PAYMENT", "IEPS", "175292049842333844342", -150.0, 5955.28),
    (7777771001, 6, datetime.date(2020, 2, 26), datetime.date(2020, 2, 26), "PAYMENT", "IEPS", "175292049842333844342", -150.0, 5805.28),
    (7777771001, 7, datetime.date(2020, 2, 26), datetime.date(2020, 2, 26), "PAYMENT", "IEPS", "175292049842333844342", -150.0, 5655.28),
    (7777771001, 8, datetime.date(2020, 2, 26), datetime.date(2020, 2, 26), "PAYMENT", "IEPS", "175292049842333844342", -150.0, 5505.28),
    (7777771001, 9, datetime.date(2020, 2, 26), datetime.date(2020, 2, 26), "PAYMENT", "IEPS", "175292049842333844342", -150.0, 5355.28),
    (7777771001, 10, datetime.date(2020, 2, 26), datetime.date(2020, 2, 26), "PAYMENT", "IEPS", "175292049842333844342", -150.0, 5205.28)
]

df = spark.createDataFrame(data, schema=schema)
df.show(truncate=False)
df.printSchema()
