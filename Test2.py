

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameMapping").getOrCreate()

# Load your data into dataframes
df1_path = '/mnt/data/file-Ui7A2cVjj2EK34peZ4TEdbq4'
df2_path = '/mnt/data/file-ga30HtRGB1Ly7fF4JDlaQ4re'
df1 = spark.read.csv(df1_path, header=True, inferSchema=True)
df2 = spark.read.csv(df2_path, header=True, inferSchema=True)

# Select relevant columns from df2
df2_select = df2.select('account_number', 'attribute', 'formatted')

# Join df1 with df2 based on tokenization and segment mapping
joined_df = df1.alias('df1').join(df2_select.alias('df2'),
                                  (col('df1.account_number') == col('df2.account_number')) &
                                  (col('df1.tokenization') == col('df2.attribute')),
                                  'left')

# Filter out unique formatted values for USTAXID and PAN
unique_ustaxid_df = joined_df.filter((col('df1.tokenization') == 'USTAXID') & (col('df1.segment').isin('BASE', 'J2'))) \
                             .select('df1.*', col('df2.formatted').alias('ustaxid_formatted')).distinct()

unique_pan_df = joined_df.filter((col('df1.tokenization') == 'PAN') & (col('df1.segment') == 'BASE')) \
                         .select('df1.*', col('df2.formatted').alias('pan_formatted')).distinct()

# Show the results
print("DataFrame with USTAXID formatted values:")
unique_ustaxid_df.show(truncate=False)

print("DataFrame with PAN formatted values:")
unique_pan_df.show(truncate=False)

# Combine results with updated formatted values
final_df = unique_ustaxid_df.unionByName(unique_pan_df)

# Show the final DataFrame
print("Final DataFrame:")
final_df.show(truncate=False)

# Stop the Spark session
spark.stop()
