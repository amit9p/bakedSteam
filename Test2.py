
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameMapping").getOrCreate()

# Load your data into dataframes (assuming the data is in CSV format)
# Replace '/mnt/data/file-Ui7A2cVjj2EK34peZ4TEdbq4' with the correct paths to your files if they are different
df1 = spark.read.csv('/mnt/data/df1.csv', header=True, inferSchema=True)
df2 = spark.read.csv('/mnt/data/df2.csv', header=True, inferSchema=True)

# Select relevant columns from df2
df2_select = df2.select('account_number', 'attribute', 'formatted')

# Join df1 with df2 based on tokenization and segment mapping
joined_df = df1.join(df2_select, (df1['account_number'] == df2_select['account_number']) & (df1['tokenization'] == df2_select['attribute']), 'left')

# Filter out unique formatted values for USTAXID and PAN
unique_ustaxid_df = joined_df.filter((col('tokenization') == 'USTAXID') & (col('segment').isin('BASE', 'J2'))).select('formatted').distinct()
unique_pan_df = joined_df.filter((col('tokenization') == 'PAN') & (col('segment') == 'BASE')).select('formatted').distinct()

# Show the results
print("Unique formatted values for USTAXID:")
unique_ustaxid_df.show()

print("Unique formatted values for PAN:")
unique_pan_df.show()

# Stop the Spark session
spark.stop()
