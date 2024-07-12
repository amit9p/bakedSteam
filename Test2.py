
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
