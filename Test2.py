
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameMerge").getOrCreate()

# Load the dataframes
df1 = spark.read.option("header", "true").csv("/mnt/data/file-C616nK97kHWBl68CqpD9VFt4")
df2 = spark.read.option("header", "true").csv("/mnt/data/file-n4ljITUBPe0nYbp1eF0NmRkG")

# Show the schemas for verification
df1.printSchema()
df2.printSchema()

# Replace 'value' column in df1 with 'Social Security Number' and 'Consumer Account Number' from df2 based on 'tokenization_type'
df1 = df1.withColumn(
    "value",
    when(col("tokenization_type") == "ustaxid", df2.select("Social Security Number").first()[0])
    .when(col("tokenization_type") == "pan", df2.select("Consumer Account Number").first()[0])
    .otherwise(col("value"))
)

# Show the resulting dataframe
df1.show()
