
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameReplacement").getOrCreate()

# Load the dataframes
df1 = spark.read.parquet("/path/to/df1.parquet")
df2 = spark.read.parquet("/path/to/df2.parquet")

# Create temporary views for SQL queries
df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")

# Update df2 with values from df1 where attribute is Social Security Number and tokenization_type is USTAXID
df2_updated_ssn = spark.sql("""
    SELECT 
        df2.account_id,
        df2.attribute,
        CASE
            WHEN df2.attribute = 'Social Security Number' AND df2.tokenization_type = 'USTAXID'
            THEN df1.value
            ELSE df2.value
        END as value,
        df2.tokenization_type
    FROM df2
    LEFT JOIN df1
    ON df2.attribute = df1.attribute
    AND df2.tokenization_type = df1.tokenization_type
    AND df2.attribute = 'Social Security Number'
    AND df2.tokenization_type = 'USTAXID'
""")

# Update df2 with values from df1 where attribute is Consumer Account Number and tokenization_type is PAN
df2_final = spark.sql("""
    SELECT 
        df2_updated_ssn.account_id,
        df2_updated_ssn.attribute,
        CASE
            WHEN df2_updated_ssn.attribute = 'Consumer Account Number' AND df2_updated_ssn.tokenization_type = 'PAN'
            THEN df1.value
            ELSE df2_updated_ssn.value
        END as value,
        df2_updated_ssn.tokenization_type
    FROM df2_updated_ssn
    LEFT JOIN df1
    ON df2_updated_ssn.attribute = df1.attribute
    AND df2_updated_ssn.tokenization_type = df1.tokenization_type
    AND df2_updated_ssn.attribute = 'Consumer Account Number'
    AND df2_updated_ssn.tokenization_type = 'PAN'
""")

# Show the updated dataframe
df2_final.show()
