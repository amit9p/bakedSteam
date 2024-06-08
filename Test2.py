

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Retrieve configuration for paths
    input_path = config.get('Paths', 'InputPath', fallback='default/path/if/not/set')
    output_dir = config.get('Paths', 'OutputDir', fallback='default/output/path/if/not/set')

    # Start the Spark session
    spark = SparkSession.builder.appName("cbr_assembler").getOrCreate()

    # Read the data
    df = spark.read.parquet(input_path)

    # Filter data for USTAXID or PAN
    t_df = df.filter((col("tokenization_type") == "USTAXID") | (col("tokenization_type") == "PAN"))

    # Select columns and remove duplicates
    t_df = t_df.select(col("value"), col("account_id")).distinct()

    # Show the DataFrame and print schema
    t_df.show()
    t_df.printSchema()

if __name__ == "__main__":
    main()
