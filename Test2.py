

from pyspark.sql import SparkSession
import boto3

def process_parquet_file(s3_bucket, parquet_key):
    # Create a SparkSession for this file
    spark = SparkSession.builder \
        .appName(f"Process_{parquet_key}") \
        .getOrCreate()
    
    # Construct the full S3 path
    s3_path = f"s3a://{s3_bucket}/{parquet_key}"
    
    # Read the Parquet file
    df = spark.read.parquet(s3_path)
    
    # Example processing (you can modify this part as needed)
    df.show()  # Display the first few rows of the DataFrame
    
    # Stop the SparkSession
    spark.stop()

def list_parquet_files_in_s3(s3_bucket, s3_prefix):
    # Initialize the S3 client
    s3_client = boto3.client('s3')
    
    # List objects within the specified S3 bucket and prefix
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    
    # Filter the keys to only include .parquet files
    parquet_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.parquet')]
    
    return parquet_files

def main():
    s3_bucket = 'your-s3-bucket-name'
    s3_prefix = 'your/folder/prefix/'  # The folder within the S3 bucket
    
    # Get the list of Parquet files in the S3 bucket
    parquet_files = list_parquet_files_in_s3(s3_bucket, s3_prefix)
    
    # Process each Parquet file sequentially
    for parquet_file in parquet_files:
        print(f"Processing file: {parquet_file}")
        process_parquet_file(s3_bucket, parquet_file)

if __name__ == "__main__":
    main()
