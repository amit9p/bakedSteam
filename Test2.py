
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameUpdate").getOrCreate()

# Load the dataframes from Parquet files
df1 = spark.read.parquet("/mnt/data/file-eqqVMGkuPK06mQbkfQDB8YDG")
df2 = spark.read.parquet("/mnt/data/file-kLerMcSpwnSvKHOLbBxSNTbd")

# Filter df1 to include only the relevant output_record_sequence values
filtered_df1 = df1.filter(df1.output_record_sequence.isin([320176, 320719]))

# Update df1 formatted field based on df2 formatted values
updated_df1 = filtered_df1.join(df2, on="tokenization", how="left") \
    .select(
        filtered_df1.business_date,
        filtered_df1.run_identifier,
        filtered_df1.output_file_type,
        filtered_df1.output_record_sequence,
        filtered_df1.output_field_sequence,
        filtered_df1.attribute,
        when(filtered_df1.tokenization == 'USTAXID', df2.formatted).when(filtered_df1.tokenization == 'PAN', df2.formatted).otherwise(filtered_df1.formatted).alias("formatted"),
        filtered_df1.tokenization,
        filtered_df1.account_number,
        filtered_df1.segment
    )

# Remove duplicates if any (optional)
updated_df1 = updated_df1.dropDuplicates()

# Show the updated dataframe
updated_df1.show(truncate=False)
