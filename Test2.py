
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameReplacement").getOrCreate()

# Load the dataframes
df1 = spark.read.parquet("/path/to/df1.parquet")
df2 = spark.read.parquet("/path/to/df2.parquet")

# Select the relevant rows from df1 for replacement
df1_ssn = df1.filter((df1["attribute"] == "Social Security Number") & (df1["tokenization_type"] == "USTAXID"))
df1_pan = df1.filter((df1["attribute"] == "Consumer Account Number") & (df1["tokenization_type"] == "PAN"))

# Join df2 with df1_ssn and df1_pan for replacement
df2_updated = df2 \
    .join(df1_ssn.select("value", "attribute", "tokenization_type"), 
          on=["attribute", "tokenization_type"], 
          how="left") \
    .withColumn("value", 
                col("df1.value").alias("df1_value")
                .when((df2["attribute"] == "Social Security Number") & (df2["tokenization_type"] == "USTAXID"), col("df1.value"))
                .otherwise(col("df2.value"))
               ) \
    .drop("df1.value")

df2_final = df2_updated \
    .join(df1_pan.select("value", "attribute", "tokenization_type"), 
          on=["attribute", "tokenization_type"], 
          how="left") \
    .withColumn("value", 
                col("df1.value").alias("df1_value")
                .when((df2_updated["attribute"] == "Consumer Account Number") & (df2_updated["tokenization_type"] == "PAN"), col("df1.value"))
                .otherwise(col("df2_updated.value"))
               ) \
    .drop("df1.value")

# Show the updated dataframe
df2_final.show()
