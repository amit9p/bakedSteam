

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

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

# Select distinct formatted values for each tokenization in df2
df2_ustaxid = df2.filter(df2.tokenization == 'USTAXID').select('formatted').distinct().withColumn("id", monotonically_increasing_id())
df2_pan = df2.filter(df2.tokenization == 'PAN').select('formatted').distinct().withColumn("id", monotonically_increasing_id())

# Add an id column to filtered_df1 for joining
filtered_df1 = filtered_df1.withColumn("id", monotonically_increasing_id())

# Join filtered_df1 with df2_ustaxid and df2_pan separately, then union the results
updated_df1_ustaxid = filtered_df1.filter(filtered_df1.tokenization == 'USTAXID') \
    .join(df2_ustaxid, "id", "left") \
    .withColumn("formatted", col("formatted").alias("formatted_ustaxid"))

updated_df1_pan = filtered_df1.filter(filtered_df1.tokenization == 'PAN') \
    .join(df2_pan, "id", "left") \
    .withColumn("formatted", col("formatted").alias("formatted_pan"))

# Union the results
updated_df1 = updated_df1_ustaxid.union(updated_df1_pan)

# Select the required columns
final_df1 = updated_df1.select(
    col("business_date"),
    col("run_identifier"),
    col("output_file_type"),
    col("output_record_sequence"),
    col("output_field_sequence"),
    col("attribute"),
    col("formatted"),
    col("tokenization"),
    col("account_number"),
    col("segment")
)

# Show the updated dataframe
final_df1.show(truncate=False)
