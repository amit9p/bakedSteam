
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize Spark session
spark = SparkSession.builder.appName("Update DataFrame Columns").getOrCreate()

# Sample data for brad_df and result_df
data_brad = [
    ("2024-03-24", "171747634674", "metro2-all", 321618, 12, "Record Descriptor Word (RDW)", "RDW123", "NULL", "1000000000321616", "BASE"),
    ("2024-03-24", "171747634674", "metro2-all", 321618, 13, "Processing Indicator", "1", "NULL", "1000000000321616", "BASE"),
    ("2024-03-24", "171747634674", "metro2-all", 321618, 14, "Time Stamp", "0808080808", "NULL", "1000000000321616", "BASE"),
    ("2024-03-24", "171747634674", "metro2-all", 321618, 15, "Reserved", "0", "NULL", "1000000000321616", "BASE"),
    ("2024-03-24", "171747634674", "metro2-all", 321618, 16, "Identification Number", "114628081", "NULL", "1000000000321616", "BASE"),
    ("2024-03-24", "171747634674", "metro2-all", 321618, 17, "Cycle Identifier", "0", "NULL", "1000000000321616", "BASE"),
    ("2024-03-24", "171747634674", "metro2-all", 321618, 18, "Consumer Account Number", "880133zeQc1o23421", "PAN", "1000000000321616", "BASE"),
    ("2024-03-24", "171747634674", "metro2-all", 321618, 19, "Portfolio Type", "I", "NULL", "1000000000321616", "BASE"),
    ("2024-03-24", "171747634674", "metro2-all", 321618, 20, "Account Type", "00", "NULL", "1000000000321616", "BASE"),
    ("2024-03-24", "171747634674", "metro2-all", 321618, 21, "Date Opened", "12262013", "NULL", "1000000000321616", "BASE")
]
columns_brad = ["business_date", "run_identifier", "output_file_type", "output_record_sequence", "output_field_sequence", "attribute", "formatted", "tokenization", "account_number", "segment"]

data_result = [
    ("1000000000321616", "Consumer Account Number", "880133zeQc1o23421", "PAN"),
    ("1000000000321616", "Social Security Number", "kxWQmI6m", "USTAXID"),
    ("1000000000323351", "Consumer Account Number", "880133zeQc1o23421", "PAN"),
    ("1000000000323351", "Social Security Number", "kxWQmI6m", "USTAXID")
]
columns_result = ["account_number", "attribute", "formatted", "tokenization"]

# Create DataFrames
brad_df = spark.createDataFrame(data_brad, columns_brad)
result_df = spark.createDataFrame(data_result, columns_result)

# Join brad_df with result_df on account_number and tokenization
joined_df = brad_df.join(result_df, on=["account_number", "tokenization"], how="left")

# Update the formatted column in brad_df with formatted from result_df
updated_df = joined_df.withColumn("formatted", when(col("result_df.formatted").isNotNull(), col("result_df.formatted")).otherwise(col("brad_df.formatted"))) \
                      .select(brad_df["*"])

# Show the result DataFrame
updated_df.show(truncate=False)

# Stop the Spark session
spark.stop()
