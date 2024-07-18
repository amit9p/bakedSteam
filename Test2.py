
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameUpdate").getOrCreate()

# Load the dataframes (assuming the files are in CSV format)
df1 = spark.read.option("header", "true").csv("/mnt/data/file-B7r0zOyD4gKniGf2f7AsFoeK")
df2 = spark.read.option("header", "true").csv("/mnt/data/file-JeAIgdbVlXAYt5jACguhXr9q")

# Create a temporary view for SQL queries
df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")

# Update df1 formatted field based on df2 formatted values
updated_df1 = spark.sql("""
    SELECT 
        df1.business_date,
        df1.run_identifier,
        df1.output_file_type,
        df1.output_record_sequence,
        df1.output_field_sequence,
        df1.attribute,
        CASE 
            WHEN df1.tokenization = 'USTAXID' THEN df2.formatted
            WHEN df1.tokenization = 'PAN' THEN df2.formatted
            ELSE df1.formatted
        END AS formatted,
        df1.tokenization,
        df1.account_number,
        df1.segment
    FROM df1
    LEFT JOIN df2
    ON df1.tokenization = df2.tokenization
""")

# Show the updated dataframe
updated_df1.show(truncate=False)
