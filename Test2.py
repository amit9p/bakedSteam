
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameMapping").getOrCreate()

# Load your data into dataframes (adjust the paths to your files)
df1_path = '/mnt/data/file-Ui7A2cVjj2EK34peZ4TEdbq4'
df2_path = '/mnt/data/file-ga30HtRGB1Ly7fF4JDlaQ4re'
df1 = spark.read.csv(df1_path, header=True, inferSchema=True)
df2 = spark.read.csv(df2_path, header=True, inferSchema=True)

# Select relevant columns from df2
df2_select = df2.select('attribute', 'formatted')

# Join df1 with df2 based on tokenization mapping
joined_df = df1.alias('df1').join(df2_select.alias('df2'),
                                  (col('df1.tokenization') == col('df2.attribute')),
                                  'left')

# Create separate DataFrames for USTAXID and PAN
ustaxid_df = joined_df.filter(col('df1.tokenization') == 'USTAXID') \
                      .select('df1.*', col('df2.formatted').alias('ustaxid_formatted'))

pan_df = joined_df.filter(col('df1.tokenization') == 'PAN') \
                  .select('df1.*', col('df2.formatted').alias('pan_formatted'))

# Join the original df1 with the USTAXID and PAN formatted values
final_df = df1.alias('df1') \
              .join(ustaxid_df.select('output_record_sequence', 'ustaxid_formatted').distinct(),
                    on='output_record_sequence', how='left') \
              .join(pan_df.select('output_record_sequence', 'pan_formatted').distinct(),
                    on='output_record_sequence', how='left')

# Show the final DataFrame
final_df.show(truncate=False)

# Stop the Spark session
spark.stop()
