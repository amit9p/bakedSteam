

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col

# Initialize Spark session
spark = SparkSession.builder.appName("RenameColumns").getOrCreate()

# Sample data for new_df (replace this with your actual DataFrame loading code)
data_new = [("1", "acc1", "seg1", "attr1", "val1", 1, 1, "type1", "2023-07-21"),
            ("2", "acc2", "TRAILER", "attr2", "val2", 2, 2, "type2", "2023-07-22")]

columns_new = ["run_identifier", "account_number", "segment", "attribute", "formatted", "output_record_sequence", "output_field_sequence", "output_file_type", "business_date"]

new_df = spark.createDataFrame(data_new, columns_new)

# Update run_identifier where segment is TRAILER
new_df = new_df.withColumn("run_identifier", when(col("segment") == "TRAILER", lit("1717436474")).otherwise(col("run_identifier")))

# Show the updated DataFrame
new_df.show()
