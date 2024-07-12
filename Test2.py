
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# Initialize Spark session
spark = SparkSession.builder.appName("UpdateDF").getOrCreate()

# Load the DataFrames from Parquet files
df1 = spark.read.parquet("/mnt/data/file-2341N8Q5Ck9XWceafQThIIBc")
df2 = spark.read.parquet("/mnt/data/file-ry7at8nK7N1dS8RxRjahVLn8")

# Rename columns in df2 for clarity
df2 = df2.withColumnRenamed("ssn", "social_security_number") \
         .withColumnRenamed("pan", "consumer_account_number")

# Create a temporary view of df2
df2.createOrReplaceTempView("df2")

# Update df1 based on df2 values and tokenization_type
df1_updated = df1 \
    .withColumn("value", 
                when((df1.attribute == "Social Security Number") & (df1.tokenization_type == "USTAXID"), 
                     col("social_security_number")).when((df1.attribute == "Consumer Account Number") & (df1.tokenization_type == "PAN"), 
                     col("consumer_account_number")).otherwise(df1.value))

# Join df1 with df2 based on the updated value column
df1_final = df1_updated.alias("df1") \
    .join(df2.alias("df2"), 
          (col("df1.value") == col("df2.social_security_number")) | 
          (col("df1.value") == col("df2.consumer_account_number")), 
          "left") \
    .select(col("df1.*"), col("df2.social_security_number"), col("df2.consumer_account_number"))

# Show the updated DataFrame
df1_final.show()
