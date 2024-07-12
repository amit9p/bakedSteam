
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("SplitDataFrame").getOrCreate()

# Load the data into a DataFrame
file_path = "/path/to/your/file"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Filter DataFrame for 'USTAXID'
ustaxid_df = df.filter(col("tokenization_type") == "USTAXID")

# Filter DataFrame for 'PAN'
pan_df = df.filter(col("tokenization_type") == "PAN")

# Show the resulting DataFrames
ustaxid_df.show()
pan_df.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, when

# Initialize Spark session
spark = SparkSession.builder.appName("AddWhiteSpaces").getOrCreate()

# Load the data into a DataFrame
file_path = "/path/to/your/file"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Add 13 white spaces to the 'value' column where 'tokenization_type' is 'PAN'
df = df.withColumn(
    "value",
    when(col("tokenization_type") == "PAN", concat(col("value"), lit(" " * 13)))
    .otherwise(col("value"))
)

# Show the resulting DataFrame
df.show()

# Save the result to a new CSV file if needed
output_path = "/path/to/output/file"
df.write.csv(output_path, header=True)
