

from pyspark.sql import SparkSession

def list_parquet_files_in_s3(spark, s3_bucket, s3_prefix):
    # Use Hadoop FileSystem API via Spark to list files in the S3 bucket and prefix
    s3_path = f"s3a://{s3_bucket}/{s3_prefix}"
    
    # List files using SparkContext's hadoop configuration
    files = spark._jsc.hadoopConfiguration().get("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(s3_path)
    file_statuses = fs.listStatus(path)
    
    # Filter the list to include only .parquet files
    parquet_files = [file_status.getPath().toString() for file_status in file_statuses if file_status.getPath().getName().endsWith(".parquet")]
    
    return parquet_files

def process_parquet_file(s3_path):
    # Create a SparkSession for this file
    spark = SparkSession.builder \
        .appName(f"Process_{s3_path.split('/')[-1]}") \
        .getOrCreate()
    
    # Read the Parquet file
    df = spark.read.parquet(s3_path)
    
    # Example processing (you can modify this part as needed)
    df.show()  # Display the first few rows of the DataFrame
    
    # Stop the SparkSession
    spark.stop()

def main():
    s3_bucket = 'your-s3-bucket-name'
    s3_prefix = 'your/folder/prefix/'  # The folder within the S3 bucket
    
    # Initialize a temporary SparkSession to list files
    spark = SparkSession.builder \
        .appName("ListS3Files") \
        .getOrCreate()
    
    # Get the list of Parquet files in the S3 bucket
    parquet_files = list_parquet_files_in_s3(spark, s3_bucket, s3_prefix)
    
    # Stop the temporary SparkSession
    spark.stop()
    
    # Process each Parquet file sequentially
    for parquet_file in parquet_files:
        print(f"Processing file: {parquet_file}")
        process_parquet_file(parquet_file)

if __name__ == "__main__":
    main()
