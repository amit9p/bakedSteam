
df2 = df2.repartition(200).withColumn("index", row_number().over(Window.partitionBy("account_id").orderBy(lit(1))))
df1 = df1.repartition(200).withColumn("index", row_number().over(Window.partitionBy("account_id").orderBy(lit(1))))


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, row_number
from pyspark.sql.window import Window

def main():
    # Initialize Spark session with configurations
    spark = SparkSession.builder \
        .appName("BatchJoin") \
        .config("spark.executor.memory", "16g") \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.broadcastTimeout", "36000") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.shuffle.partitions", 200) \
        .getOrCreate()

    # Define the schema for df1
    schema = StructType([
        StructField("account_id", StringType(), nullable=True),
        StructField("attribute", StringType(), nullable=True),
        StructField("value", StringType(), nullable=True),
        StructField("tokenization_type", StringType(), nullable=True)
    ])

    # Load the DataFrames from Parquet files
    df2 = spark.read.parquet("/Users/vmq634/PycharmProjects/oscar_test/ecbr_assembler/data/input/tokenized_sample/ECBR_TRANSUNOTOKENIZED")
    df1 = spark.read.schema(schema).parquet("/Users/vmq634/PycharmProjects/oscar_test/ecbr_assembler/ecbr_assembler/tokenized_ustaxid_pan/pan-pan-all")

    # Add an index column to both DataFrames
    windowSpec = Window.orderBy(lit(1))
    df2 = df2.withColumn("index", row_number().over(windowSpec))
    df1 = df1.withColumn("index", row_number().over(windowSpec))

    # Define batch size
    batch_size = 1000000  # Adjust based on your memory limits

    # Get the total number of rows
    max_index_df2 = df2.count()
    max_index_df1 = df1.count()
    max_index = max(max_index_df2, max_index_df1)

    # Function to process each batch
    def process_batch(batch_df2, batch_df1, batch_id):
        # Filter and rename columns for SSN
        df1_ssn = batch_df1.filter(
            (col("attribute") == "Social Security Number") &
            (col("tokenization_type") == "USTAXID")
        ).select(
            col("value").alias("ssn_value"),
            col("attribute").alias("ssn_attribute"),
            col("tokenization_type").alias("ssn_tokenization_type")
        )

        # Join and replace SSN values
        df2_with_ssn = batch_df2.join(
            df1_ssn,
            (batch_df2["attribute"] == df1_ssn["ssn_attribute"]) &
            (batch_df2["tokenization_type"] == df1_ssn["ssn_tokenization_type"]),
            "left"
        ).withColumn(
            "value",
            when(
                (col("attribute") == "Social Security Number") &
                (col("tokenization_type") == "USTAXID"),
                col("ssn_value")
            ).otherwise(col("value"))
        ).drop("ssn_attribute", "ssn_tokenization_type", "ssn_value")

        # Filter and rename columns for PAN
        df1_pan = batch_df1.filter(
            (col("attribute") == "Consumer Account Number") &
            (col("tokenization_type") == "PAN")
        ).select(
            col("value").alias("pan_value"),
            col("attribute").alias("pan_attribute"),
            col("tokenization_type").alias("pan_tokenization_type")
        )

        # Join and replace PAN values
        df2_final = df2_with_ssn.join(
            df1_pan,
            (df2_with_ssn["attribute"] == df1_pan["pan_attribute"]) &
            (df2_with_ssn["tokenization_type"] == df1_pan["pan_tokenization_type"]),
            "left"
        ).withColumn(
            "value",
            when(
                (col("attribute") == "Consumer Account Number") &
                (col("tokenization_type") == "PAN"),
                col("pan_value")
            ).otherwise(col("value"))
        ).drop("pan_attribute", "pan_tokenization_type", "pan_value")

        # Show the updated DataFrame
        df2_final.show()

        # Save the resulting DataFrame
        output_path = f"/path/to/output/directory/batch_{batch_id}.parquet"
        df2_final.write.mode("overwrite").parquet(output_path)

    # Process in batches
    for i in range(0, max_index, batch_size):
        batch_df2 = df2.filter((col("index") > i) & (col("index") <= i + batch_size)).drop("index")
        batch_df1 = df1.filter((col("index") > i) & (col("index") <= i + batch_size)).drop("index")
        process_batch(batch_df2, batch_df1, i // batch_size)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
