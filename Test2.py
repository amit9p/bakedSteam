

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
df2_ustaxid = df2.filter(df2.tokenization == 'USTAXID').select('formatted').distinct()
df2_pan = df2.filter(df2.tokenization == 'PAN').select('formatted').distinct()

# Add an index column to df2_ustaxid and df2_pan to join them correctly
df2_ustaxid = df2_ustaxid.withColumn("index", col("formatted").substr(-1, 1).cast("int"))
df2_pan = df2_pan.withColumn("index", col("formatted").substr(-1, 1).cast("int"))

# Add an index column to filtered_df1 to match df2
filtered_df1 = filtered_df1.withColumn("index", col("output_field_sequence"))

# Join filtered_df1 with df2_ustaxid and df2_pan separately, then union the results
updated_df1_ustaxid = filtered_df1.filter(filtered_df1.tokenization == 'USTAXID').join(df2_ustaxid, "index", "left").drop("formatted").withColumnRenamed("formatted", "new_formatted")
updated_df1_pan = filtered_df1.filter(filtered_df1.tokenization == 'PAN').join(df2_pan, "index", "left").drop("formatted").withColumnRenamed("formatted", "new_formatted")

# Union the results
updated_df1 = updated_df1_ustaxid.union(updated_df1_pan)

# Select the required columns and update the formatted column
final_df1 = updated_df1.select(
    col("business_date"),
    col("run_identifier"),
    col("output_file_type"),
    col("output_record_sequence"),
    col("output_field_sequence"),
    col("attribute"),
    col("new_formatted").alias("formatted"),
    col("tokenization"),
    col("account_number"),
    col("segment")
)

# Show the updated dataframe
final_df1.show(truncate=False)
