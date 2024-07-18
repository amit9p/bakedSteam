
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit

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

# Add a unique key for joining by combining output_record_sequence and tokenization
filtered_df1 = filtered_df1.withColumn("join_key", concat(col("output_record_sequence").cast("string"), col("tokenization")))
df2 = df2.withColumn("join_key", lit(None).cast("string"))

# Create a unique join_key for df2 by assigning sequential keys
ustaxid_key = filtered_df1.filter(filtered_df1.tokenization == 'USTAXID').select("join_key").distinct()
pan_key = filtered_df1.filter(filtered_df1.tokenization == 'PAN').select("join_key").distinct()

# Assign join keys to df2
df2_ustaxid = df2.filter(df2.tokenization == 'USTAXID').withColumn("join_key", lit(ustaxid_key.collect()[0][0]))
df2_pan = df2.filter(df2.tokenization == 'PAN').withColumn("join_key", lit(pan_key.collect()[0][0]))

# Union the updated df2
df2 = df2_ustaxid.union(df2_pan)

# Join filtered_df1 with df2 on the unique join_key
updated_df1 = filtered_df1.join(df2, "join_key", "left").drop("join_key")

# Select the required columns and update the formatted column
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
