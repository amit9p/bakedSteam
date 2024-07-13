

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameReplacement").getOrCreate()

# Load the dataframes
df1 = spark.read.parquet("/mnt/data/file-U4qkPznEyieGeGhGQns07nx7")
df2 = spark.read.parquet("/mnt/data/file-C5CTELwnzCKFOajIoqYsj7rk")

# Filter and rename columns for SSN
df1_ssn = df1.filter((df1["attribute"] == "Social Security Number") & (df1["tokenization_type"] == "USTAXID")) \
             .select(col("value").alias("ssn_value"), col("attribute").alias("ssn_attribute"), col("tokenization_type").alias("ssn_tokenization_type"))

# Join and replace SSN values
df2_with_ssn = df2.join(df1_ssn, 
                        (df2["attribute"] == df1_ssn["ssn_attribute"]) & (df2["tokenization_type"] == df1_ssn["ssn_tokenization_type"]), 
                        "left") \
                  .withColumn("value", 
                              when((col("attribute") == "Social Security Number") & (col("tokenization_type") == "USTAXID"), 
                                   col("ssn_value")).otherwise(col("value"))) \
                  .drop("ssn_attribute").drop("ssn_tokenization_type").drop("ssn_value")

# Filter and rename columns for PAN
df1_pan = df1.filter((df1["attribute"] == "Consumer Account Number") & (df1["tokenization_type"] == "PAN")) \
             .select(col("value").alias("pan_value"), col("attribute").alias("pan_attribute"), col("tokenization_type").alias("pan_tokenization_type"))

# Join and replace PAN values
df2_final = df2_with_ssn.join(df1_pan, 
                              (df2_with_ssn["attribute"] == df1_pan["pan_attribute"]) & (df2_with_ssn["tokenization_type"] == df1_pan["pan_tokenization_type"]), 
                              "left") \
                        .withColumn("value", 
                                    when((col("attribute") == "Consumer Account Number") & (col("tokenization_type") == "PAN"), 
                                         col("pan_value")).otherwise(col("value"))) \
                        .drop("pan_attribute").drop("pan_tokenization_type").drop("pan_value")

# Show the updated dataframe
df2_final.show()
