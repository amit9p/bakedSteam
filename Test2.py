
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("ReplaceFormattedValues").getOrCreate()

# Assuming df_a and df_b are your DataFrames representing Table A and Table B
# Load your DataFrames here, replace this with the actual loading logic
df_a = # Load Table A DataFrame
df_b = # Load Table B DataFrame

# Filter Table B for specific conditions
df_b_filtered = df_b.filter(
    ((col('tokenization') == 'USTAXID') & col('output_field_sequence').isin(35, 54)) |
    ((col('tokenization') == 'PAN') & (col('output_field_sequence') == 7))
)

# Join Table A with the filtered Table B on account_number, tokenization, and output_field_sequence
df_joined = df_a.alias("df_a").join(df_b_filtered.alias("df_b"), on=['account_number', 'tokenization', 'output_field_sequence'], how='left')

# Replace the formatted values in Table A with the corresponding values from Table B
df_replaced = df_joined.withColumn(
    'formatted_final',
    when(
        ((col('df_a.tokenization') == 'USTAXID') & col('df_a.output_field_sequence').isin(35, 54)) |
        ((col('df_a.tokenization') == 'PAN') & (col('df_a.output_field_sequence') == 7)),
        col('df_b.formatted')
    ).otherwise(col('df_a.formatted'))
)

# Drop the original `formatted` columns to remove ambiguity
df_final = df_replaced.drop('df_a.formatted').drop('df_b.formatted')

# Rename the final formatted column to `formatted`
df_final = df_final.withColumnRenamed('formatted_final', 'formatted')

# Select only the necessary columns from the final DataFrame
df_final = df_final.select(
    'business_date', 
    'run_identifier', 
    'output_file_type', 
    'output_record_sequence',
    'output_field_sequence', 
    'attribute', 
    'formatted', 
    'tokenization', 
    'account_number', 
    'segment'
)

# Show the final DataFrame (optional)
df_final.show()

# You can also write the final DataFrame to a file or save it back to your storage
# df_final.write.format("csv").option("header", "true").save("/path/to/save/final_df.csv")

# Stop the Spark session
spark.stop()
