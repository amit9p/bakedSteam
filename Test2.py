

from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("CleanCSV").getOrCreate()

# Input paths
account_dataset = "/Users/yourpath/account_dataset.csv"
customer_dataset = "/Users/yourpath/customer_dataset.csv"

# Output paths
account_output = "/Users/yourpath/output_account.csv"
customer_output = "/Users/yourpath/output_customer.csv"

# Load datasets
account_df = spark.read.option("header", True).csv(account_dataset)
customer_df = spark.read.option("header", True).csv(customer_dataset)

# Field to move
field_to_move = "credit_report_subject_social_security_number"

# Extract and drop the field
field_col = account_df.select(field_to_move)
account_df = account_df.drop(field_to_move)

# Add to customer_df
from pyspark.sql.functions import lit

# This assumes the order of rows is consistent between datasets
customer_df = customer_df.withColumn(field_to_move, field_col[field_to_move])

# Write outputs as single files
account_df.coalesce(1).write.mode("overwrite").option("header", True).csv(account_output)
customer_df.coalesce(1).write.mode("overwrite").option("header", True).csv(customer_output)
