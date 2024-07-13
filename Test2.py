
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameReplacement").getOrCreate()

# Load the dataframes
df1 = spark.read.parquet("/mnt/data/file-U4qkPznEyieGeGhGQns07nx7")
df2 = spark.read.parquet("/mnt/data/file-C5CTELwnzCKFOajIoqYsj7rk")

# Select the relevant rows from df1 for replacement
df1_ssn = df1.filter((df1["attribute"] == "Social Security Number") & (df1["tokenization_type"] == "USTAXID"))
df1_pan = df1.filter((df1["attribute"] == "Consumer Account Number") & (df1["tokenization_type"] == "PAN"))

# Join df2 with df1_ssn for Social Security Number replacement
df2_with_ssn = df2.alias("df2").join(df1_ssn.alias("df1_ssn"), 
                                     (col("df2.attribute") == col("df1_ssn.attribute")) & 
                                     (col("df2.tokenization_type") == col("df1_ssn.tokenization_type")), 
                                     "left") \
                               .withColumn("value", 
                                           when((col("df2.attribute") == "Social Security Number") & 
                                                (col("df2.tokenization_type") == "USTAXID"), 
                                                col("df1_ssn.value")).otherwise(col("df2.value"))) \
                               .drop("df1_ssn.value")

# Join the intermediate result with df1_pan for Consumer Account Number replacement
df2_final = df2_with_ssn.alias("df2").join(df1_pan.alias("df1_pan"), 
                                           (col("df2.attribute") == col("df1_pan.attribute")) & 
                                           (col("df2.tokenization_type") == col("df1_pan.tokenization_type")), 
                                           "left") \
                                     .withColumn("value", 
                                                 when((col("df2.attribute") == "Consumer Account Number") & 
                                                      (col("df2.tokenization_type") == "PAN"), 
                                                      col("df1_pan.value")).otherwise(col("df2.value"))) \
                                     .drop("df1_pan.value")

# Show the updated dataframe
df2_final.show()
