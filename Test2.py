
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameReplacement").getOrCreate()

# Load the dataframes
df1 = spark.read.parquet("/path/to/df1.parquet")
df2 = spark.read.parquet("/path/to/df2.parquet")

# Update df2 with values from df1 where attribute is Social Security Number and tokenization_type is USTAXID
df2_updated = df2.withColumn(
    "value",
    when(
        (df2["attribute"] == "Social Security Number") & (df2["tokenization_type"] == "USTAXID"),
        df1.filter(
            (df1["attribute"] == "Social Security Number") & (df1["tokenization_type"] == "USTAXID")
        ).select("value").first()[0]
    ).otherwise(df2["value"])
)

# Update df2 with values from df1 where attribute is Consumer Account Number and tokenization_type is PAN
df2_final = df2_updated.withColumn(
    "value",
    when(
        (df2_updated["attribute"] == "Consumer Account Number") & (df2_updated["tokenization_type"] == "PAN"),
        df1.filter(
            (df1["attribute"] == "Consumer Account Number") & (df1["tokenization_type"] == "PAN")
        ).select("value").first()[0]
    ).otherwise(df2_updated["value"])
)

# Show the updated dataframe
df2_final.show()
