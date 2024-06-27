
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Define the expected schema
expected_schema = StructType([
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

def test_read_parquet_file_schema(spark):
    path = "tests/resources/input/TKNZD_SAMPLE.parquet"
    df = parquet_reader.read_parquet_file(spark, path)
    
    # Get the actual schema
    actual_schema = df.schema
    
    # Compare the schemas
    assert actual_schema == expected_schema, f"Expected schema: {expected_schema}, but got: {actual_schema}"

if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    pytest.main()
