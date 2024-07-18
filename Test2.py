

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameUpdate").getOrCreate()

# Load the dataframes from Parquet files
df1 = spark.read.parquet("/mnt/data/file-eqqVMGkuPK06mQbkfQDB8YDG")
df2 = spark.read.parquet("/mnt/data/file-kLerMcSpwnSvKHOLbBxSNTbd")

# Filter df1 to include only the relevant output_record_sequence values and tokenization
filtered_df1 = df1.filter(
    (df1.output_record_sequence.isin([320176, 320719])) &
    (df1.tokenization.isin(['USTAXID', 'PAN']))
)

# Filter df2 separately for USTAXID and PAN
df2_ustaxid = df2.filter(df2.tokenization == 'USTAXID').select('formatted').distinct().limit(1)
df2_pan = df2.filter(df2.tokenization == 'PAN').select('formatted').distinct().limit(1)

# Collect the formatted values
ustaxid_formatted = df2_ustaxid.collect()[0]['formatted']
pan_formatted = df2_pan.collect()[0]['formatted']

# Update df1 formatted field based on collected formatted values
updated_df1 = filtered_df1.withColumn(
    'formatted', 
    when(filtered_df1.tokenization == 'USTAXID', ustaxid_formatted)
    .when(filtered_df1.tokenization == 'PAN', pan_formatted)
    .otherwise(filtered_df1.formatted)
)

# Show the updated dataframe
updated_df1.show(truncate=False)
