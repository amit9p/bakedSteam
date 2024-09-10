
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize SparkSession for testing
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

# Assuming the batch_process function is imported from the file/module you're testing
# from your_module import batch_process

@patch("your_module.TuringPySparkClient")  # Replace 'your_module' with the correct module name
def test_batch_process(mock_client, spark):
    # Create mock data for PySpark DataFrame
    data = [Row(col1=1, col2=3), Row(col1=2, col2=4)]
    mock_df = spark.createDataFrame(data)

    # Create mock credentials, env, and tokenization
    mock_dev_creds = {
        "client_id": "mock_client_id",
        "client_secret": "mock_client_secret"
    }
    mock_env = "mock_env"
    mock_tokenization = "USTAXID"

    # Mock the TuringPySparkClient and its process method
    mock_instance = MagicMock()
    mock_client.return_value = mock_instance
    mock_instance.process.return_value = mock_df  # Mock process method to return the input DataFrame

    # Call the batch_process function with mock inputs
    result = batch_process(mock_df, mock_dev_creds, mock_env, mock_tokenization)

    # Assertions to check if client and process were called correctly
    mock_client.assert_called_once()  # Ensure TuringPySparkClient is called
    mock_instance.process.assert_called_once()  # Ensure process method is called

    # Validate that the returned DataFrame is the same as the mocked return value
    assert result.collect() == mock_df.collect()  # Compare DataFrames by collecting them to rows
