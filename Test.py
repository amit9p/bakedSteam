

# test_assembler.py located in tests folder

import pytest
from pyspark.sql import SparkSession
from ecb_assmbler.assembler import Assembler  # Update this import based on your actual module structure

# Setup a fixture for Spark session that can be reused
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("Test Assembler") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

# Test the read_parquet_based_on_date_and_runid method
def test_read_parquet_based_on_date_and_runid(spark):
    assembler = Assembler(spark)
    path = "path/to/your/test/data.parquet"  # Ensure this points to a valid test Parquet file
    business_date = "2024-03-04"
    run_id = "sample_run_id"
    file_type = "ALL"

    # Invoke the method
    result = assembler.read_parquet_based_on_date_and_runid(path, business_date, run_id, file_type)

    # Assert conditions based on expected outcomes
    assert isinstance(result, dict)  # Check if the result is a dictionary
    assert "metro2-all" in result  # Check if 'metro2-all' DataFrame is in results

    # Further checks can be based on the content of the DataFrames
    # For example:
    assert not result["metro2-all"].rdd.isEmpty()  # Ensure the DataFrame is not empty
    # Add more assertions as necessary to validate DataFrame structures, column names, row counts, etc.
