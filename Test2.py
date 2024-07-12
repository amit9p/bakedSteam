
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit

# Initialize Spark session
spark = SparkSession.builder.appName("AddSpacesToPAN").getOrCreate()

# Assuming result_df is already created from the previous code
# Add 13 spaces to the end of each pan number
result_df = result_df.withColumn("pan", concat(col("pan"), lit(" " * 13)))

# Show the updated DataFrame
result_df.show(1000, False)
