

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

# Join filtered_df1 with df2_ustaxid and df2_pan separately
updated_df1_ustaxid = filtered_df1.filter(filtered_df1.tokenization == 'USTAXID').alias("df1") \
    .join(df2_ustaxid.alias("df2"), "id", "left") \
    .select(
        col("df1.business_date"),
        col("df1.run_identifier"),
        col("df1.output_file_type"),
        col("df1.output_record_sequence"),
        col("df1.output_field_sequence"),
        col("df1.attribute"),
        col("df2.formatted").alias("formatted"),
        col("df1.tokenization"),
        col("df1.account_number"),
        col("df1.segment")
    )

updated_df1_pan = filtered_df1.filter(filtered_df1.tokenization == 'PAN').alias("df1") \
    .join(df2_pan.alias("df2"), "id", "left") \
    .select(
        col("df1.business_date"),
        col("df1.run_identifier"),
        col("df1.output_file_type"),
        col("df1.output_record_sequence"),
        col("df1.output_field_sequence"),
        col("df1.attribute"),
        col("df2.formatted").alias("formatted"),
        col("df1.tokenization"),
        col("df1.account_number"),
        col("df1.segment")
    )

# Union the results
final_df1 = updated_df1_ustaxid.union(updated_df1_pan)

# Show the updated dataframe
final_df1.show(truncate=False)
