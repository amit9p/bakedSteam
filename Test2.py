
def test_read_parquet_file_invalid_format(spark):
    # Path to a file with an invalid format (e.g., a text file or CSV instead of a Parquet file)
    path = "tests/resources/input/INVALID_FORMAT.txt"
    
    # Attempt to read the invalid format file
    df = read_parquet_file(spark, path)
    
    # Assert that the DataFrame is None due to invalid format
    assert df is None, "Expected None for invalid format file, but got a DataFrame"

# Example of the read_parquet_file function (from parquet_reader module)
def read_parquet_file(spark, path):
    try:
        schema = StructType([
            StructField("account_id", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("segment", StringType(), True),
            StructField("attribute", StringType(), True),
            StructField("value", StringType(), True),
            StructField("row_sequence", LongType(), True),
            StructField("column_sequence", LongType(), True),
            StructField("site_type", StringType(), True),
            StructField("business_date", StringType(), True),
            StructField("__index_level_0__", LongType(), True),
            StructField("tokenization_type", StringType(), True)
        ])
        
        df = spark.read.schema(schema).parquet(path)
        df.printSchema()
        return df
    except Exception as e:
        logger.error(e)
        return None

if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    pytest.main()
