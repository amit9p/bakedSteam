

import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch
from assembler import Assembler

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("TestSession") \
        .getOrCreate()

@pytest.fixture
def assembler(spark_session):
    return Assembler(spark_session)

def create_sample_data(spark_session):
    sample_data = [
        ("2023-01-01", "1", "EQ", {"data": "example1"}),
        ("2023-01-01", "1", "TU", {"data": "example2"}),
        ("2023-01-02", "2", "EQ", {"data": "example3"})
    ]
    schema = "business_date string, run_id string, file_type string, data map<string,string>"
    return spark_session.createDataFrame(sample_data, schema)

@pytest.mark.parametrize("test_date, test_run_id, expected_count", [
    ("2023-01-01", "1", 2),
    ("2023-01-02", "2", 1),
    ("2023-01-01", "2", 0)  # No data for this run_id on this date
])
def test_read_parquet_based_on_date_and_runid(assembler, spark_session, test_date, test_run_id, expected_count):
    # Using patch to mock spark.read.parquet
    with patch.object(spark_session.read, 'parquet', return_value=create_sample_data(spark_session)) as mock_parquet:
        df = assembler.read_parquet_based_on_date_and_runid("dummy_path", test_date, test_run_id)
        assert df.count() == expected_count
        mock_parquet.assert_called_once_with("dummy_path")  # Check if our mock was called correctly
