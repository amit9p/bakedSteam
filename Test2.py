
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark
spark = SparkSession.builder.appName("CleanCSV").getOrCreate()

# Load CSVs
account_df = spark.read.option("header", True).csv("/your/path/account_dataset.csv")
customer_df = spark.read.option("header", True).csv("/your/path/customer_dataset.csv")

# Field to move
field_to_move = "credit_report_subject_social_security_number"

# Select just that column
field_column_df = account_df.select(field_to_move)

# Drop it from account_df
account_df = account_df.drop(field_to_move)

# Add it to customer_df using row-wise zip and select
from pyspark.sql.functions import monotonically_increasing_id

# Add row index to both
customer_df = customer_df.withColumn("row_idx", monotonically_increasing_id())
field_column_df = field_column_df.withColumn("row_idx", monotonically_increasing_id())

# Join by row index
customer_df = customer_df.join(field_column_df, on="row_idx", how="left").drop("row_idx")

# Write outputs
account_df.coalesce(1).write.mode("overwrite").option("header", True).csv("/your/output/account")
customer_df.coalesce(1).write.mode("overwrite").option("header", True).csv("/your/output/customer")
