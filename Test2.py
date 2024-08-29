
from pyspark import SparkConf, SparkContext
from awsglue.context import GlueContext  # Assuming you are using AWS Glue

def main():
    # Step 2: Create a SparkConf object
    conf = SparkConf() \
        .setAppName("MyGlueJob") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g") \
        .set("spark.sql.shuffle.partitions", "10") \
        .set("spark.some.config.option", "config-value")  # Add more configurations as needed

    # Step 3: Initialize SparkContext with the configured SparkConf
    sc = SparkContext.getOrCreate(conf=conf)

    # Initialize GlueContext with SparkContext
    glueContext = GlueContext(sc)

    # Your existing code
    args = getResolvedOptions(sys.argv, options=["INPUT_S3_PATH", "OUTPUT_S3_PATH", "env"])
    INPUT_S3_PATH = args["INPUT_S3_PATH"]
    OUTPUT_S3_PATH = args["OUTPUT_S3_PATH"]
    env = args["env"]

    # Continue with the rest of your job logic...

if __name__ == "__main__":
    main()
