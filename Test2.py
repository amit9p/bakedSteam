

from pyspark.sql import SparkSession

def list_files_in_s3_folder(spark, s3_path):
    # Get the Hadoop FileSystem object
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    
    # Create a Path object for the S3 folder
    path = spark._jvm.org.apache.hadoop.fs.Path(s3_path)
    
    # List files in the specified S3 folder
    file_statuses = fs.listStatus(path)
    
    # Extract the paths of the files
    files = [file_status.getPath().toString() for file_status in file_statuses]
    
    return files

def main():
    # Assuming your SparkSession is already created
    spark = SparkSession.builder.appName("ListS3Files").getOrCreate()
    
    # Define the S3 path (bucket and folder)
    s3_bucket = "your-s3-bucket-name"
    s3_prefix = "your/folder/prefix/"
    s3_path = f"s3a://{s3_bucket}/{s3_prefix}"
    
    # List files in the S3 folder
    files = list_files_in_s3_folder(spark, s3_path)
    
    # Print the list of files
    for file in files:
        print(file)
    
    # Stop the SparkSession if no further processing is needed
    spark.stop()

if __name__ == "__main__":
    main()
