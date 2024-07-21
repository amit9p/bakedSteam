from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Data Transformation") \
    .getOrCreate()

# Load df1 and df2 from their respective files
df1 = spark.read.format("parquet").load("/mnt/data/file-BkJukAK335fxnuqKD3xqmclt")
df2 = spark.read.format("parquet").load("/mnt/data/file-BzVXdsyCTQqNeaWvJOD3W2AK")

# Create temporary views for SQL queries
df1.createOrReplaceTempView("df1")
df2.createOrReplaceTempView("df2")



query = """
SELECT
    df2.business_date,
    df2.run_identifier,
    df2.output_file_type,
    df2.output_record_sequence,
    df2.output_field_sequence,
    df2.attribute,
    df2.formatted,
    df1.value AS tokenization,  -- df2.tokenization will be replaced with df1.value where the join conditions are met
    df2.account_number,
    df2.segment
FROM
    df2
LEFT JOIN
    df1
ON
    df1.segment = 'TRAILER'
    AND df1.row_position = df2.output_record_sequence
    AND df1.column_position = df2.output_field_sequence
    AND df1.run_id = df2.run_identifier
    AND df1.account_id = df2.account_number
"""

# Execute the query
new_df = spark.sql(query)

# Show the results
new_df.show(n=500, truncate=False)
new_df.printSchema()
