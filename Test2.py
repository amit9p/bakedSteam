

from pyspark.sql import SparkSession

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

# Register DataFrames as temporary views
df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")

# Use SQL to join and select the desired columns
query = """
SELECT DISTINCT df1.account_number, df2.attribute, df2.formatted, df2.tokenization
FROM df2
JOIN df1 ON df2.tokenization = df1.tokenization
WHERE df1.account_number IN (
    SELECT account_number
    FROM (
        SELECT account_number, tokenization, ROW_NUMBER() OVER (PARTITION BY tokenization ORDER BY output_record_sequence) as rn
        FROM df1
    ) subquery
    WHERE rn <= 2
)
"""

result_df = spark.sql(query)

# Show the result
result_df.show(truncate=False)
