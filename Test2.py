

from pyspark.sql import SparkSession

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

# Register DataFrames as temp views
brad_df.createOrReplaceTempView("brad_df")
result_df.createOrReplaceTempView("result_df")

# Perform SQL join and update using Spark SQL
updated_df = spark.sql("""
    SELECT
        b.business_date,
        b.run_identifier,
        b.output_file_type,
        b.output_record_sequence,
        b.output_field_sequence,
        b.attribute,
        COALESCE(r.formatted, b.formatted) AS formatted,
        b.tokenization,
        b.account_number,
        b.segment
    FROM
        brad_df b
    LEFT JOIN
        result_df r
    ON
        b.account_number = r.account_number AND b.tokenization = r.tokenization
""")

# Show the result DataFrame
updated_df.show(truncate=False)

# Stop the Spark session
spark.stop()
