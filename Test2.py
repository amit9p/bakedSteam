

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameMapping").getOrCreate()

# Load your data into dataframes
df1_path = '/mnt/data/file-Ui7A2cVjj2EK34peZ4TEdbq4'
df2_path = '/mnt/data/file-ga30HtRGB1Ly7fF4JDlaQ4re'
df1 = spark.read.csv(df1_path, header=True, inferSchema=True)
df2 = spark.read.csv(df2_path, header=True, inferSchema=True)

# Select relevant columns from df2
df2_select = df2.select('attribute', 'formatted').distinct()

# Join df1 with df2 based on tokenization mapping
joined_df = df1.alias('df1').join(df2_select.alias('df2'),
                                  (col('df1.tokenization') == col('df2.attribute')),
                                  'left')

# Update the formatted column in df1 based on the joined results
updated_df = joined_df.withColumn(
    'formatted',
    when(col('df1.tokenization') == 'USTAXID', col('df2.formatted'))
    .when(col('df1.tokenization') == 'PAN', col('df2.formatted'))
    .otherwise(col('df1.formatted'))
).select('df1.business_date', 'df1.run_identifier', 'df1.output_file_type', 
         'df1.output_record_sequence', 'df1.output_field_sequence', 
         'df1.attribute', 'formatted', 'df1.tokenization', 
         'df1.account_number', 'df1.segment')

# Show the final DataFrame with updated formatted column
updated_df.show(truncate=False)

# Save the result to a new CSV file if needed
updated_df.write.csv('/mnt/data/updated_df.csv', header=True)

# Stop the Spark session
spark.stop()
