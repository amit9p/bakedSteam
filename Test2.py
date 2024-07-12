
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("UpdateDF").getOrCreate()

# Load the DataFrames from Parquet files
df1 = spark.read.parquet("/mnt/data/file-2341N8Q5Ck9XWceafQThIIBc")
df2 = spark.read.parquet("/mnt/data/file-ry7at8nK7N1dS8RxRjahVLn8")

# Rename columns in df2 for clarity
df2 = df2.withColumnRenamed("ssn", "social_security_number") \
         .withColumnRenamed("pan", "consumer_account_number")

# Update df1 based on df2 values
df1_updated = df1 \
    .join(df2, df1.account_id == df2.account_id, "left") \
    .withColumn("value", 
                when(df1.attribute == "Social Security Number", df2.social_security_number)
                .when(df1.attribute == "Consumer Account Number", df2.consumer_account_number)
                .otherwise(df1.value)) \
    .select(df1.columns)

# Show the updated DataFrame
df1_updated.show()
