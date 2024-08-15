

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize the Glue context and Spark session
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 path where the Parquet file is located
s3_path = "s3://your-bucket-name/your-folder/your-file.parquet"

# Read the Parquet file into a DataFrame
df = spark.read.parquet(s3_path)

# If you need to convert the DataFrame to a Glue DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Perform transformations (optional)
# For example, filtering rows where a specific column has a certain value
filtered_dynamic_frame = Filter.apply(frame=dynamic_frame, f=lambda x: x["column_name"] == "some_value")

# Convert the filtered DynamicFrame back to a DataFrame (optional)
filtered_df = filtered_dynamic_frame.toDF()

# Show the data (optional)
filtered_df.show()

# Write the result back to another Parquet file or any other supported format (optional)
output_path = "s3://your-bucket-name/your-output-folder/"
filtered_df.write.mode("overwrite").parquet(output_path)

# Commit the job
job.commit()
