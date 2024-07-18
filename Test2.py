

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

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

# Add a row number to both dataframes to maintain uniqueness
window_spec = Window.partitionBy("output_record_sequence", "tokenization").orderBy("formatted")

df1_with_row_num = filtered_df1.withColumn("row_num", row_number().over(window_spec))
df2_with_row_num = df2.withColumn("row_num", row_number().over(window_spec))

# Join df1 with df2 on tokenization and row_num to get the correct mapping
updated_df1 = df1_with_row_num.join(
    df2_with_row_num,
    (df1_with_row_num.tokenization == df2_with_row_num.tokenization) & (df1_with_row_num.row_num == df2_with_row_num.row_num),
    "left"
).select(
    df1_with_row_num.business_date,
    df1_with_row_num.run_identifier,
    df1_with_row_num.output_file_type,
    df1_with_row_num.output_record_sequence,
    df1_with_row_num.output_field_sequence,
    df1_with_row_num.attribute,
    col("df2_with_row_num.formatted").alias("formatted"),
    df1_with_row_num.tokenization,
    df1_with_row_num.account_number,
    df1_with_row_num.segment
)

# Drop the row_num column
final_df1 = updated_df1.drop("row_num")

# Show the updated dataframe
final_df1.show(truncate=False)
