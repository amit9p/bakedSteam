


import datetime
from pyspark.sql import Row, types as T

schema = T.StructType([
    T.StructField("account_id", T.LongType(), True),
    T.StructField("recap_sequence", T.IntegerType(), True),
    T.StructField("transaction_posting_date", T.DateType(), True),
    T.StructField("transaction_date", T.DateType(), True),
    T.StructField("transaction_category", T.StringType(), True),
    T.StructField("transaction_source", T.StringType(), True),
    T.StructField("transaction_description", T.StringType(), True),
    T.StructField("transaction_amount", T.DoubleType(), True),
    T.StructField("transaction_resulting_balance", T.DoubleType(), True),
])

rows = [
    (7777771001, 1, datetime.date(2020,2,26), datetime.date(2020,2,26),
     "PAYMENT", "IEPS", "175292049842333844342", -150.0, 6555.28),
    (7777771001, 2, datetime.date(2020,2,27), datetime.date(2020,2,27),
     "PAYMENT", "IEPS", "175292049842333844342", -200.0, 6355.28),
]

df = spark.createDataFrame(rows, schema=schema)

df.printSchema()
df.show(truncate=False)
