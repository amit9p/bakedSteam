
import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch
from ecbr_assembler.metro2_enrichment import replace_tokenized_values  # Ensure this matches your actual module import path

# Create a Spark session
@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[2]").appName("pytest").getOrCreate()

# Test data
@pytest.fixture
def df_input(spark):
    data = [
        (1, "token_1", "formatted_1"),
        (2, "token_2", "formatted_2"),
        (3, "token_3", "formatted_3")
    ]
    schema = ["account_number", "tokenization", "formatted"]
    return spark.createDataFrame(data, schema)

@pytest.fixture
def token_cache(spark):
    data = [
        (1, "token_1", "plain_text_1"),
        (2, "token_2", "plain_text_2"),
        (3, "token_3", "plain_text_3")
    ]
    schema = ["account_number", "tokenization", "plain_text"]
    return spark.createDataFrame(data, schema)

def assert_dataframe_equality(df1, df2):
    df1_sorted = df1.sort("account_number", "tokenization")
    df2_sorted = df2.sort("account_number", "tokenization")
    assert df1_sorted.collect() == df2_sorted.collect()

def test_replace_tokenized_values_exception_handling(spark, df_input, token_cache, caplog):
    # Patch a method that is called within replace_tokenized_values to raise an exception
    with patch('ecbr_assembler.metro2_enrichment.replace_tokenized_values') as mock_replace_tokenized_values:
        mock_replace_tokenized_values.side_effect = Exception("Test exception")

        with pytest.raises(Exception) as exc_info:
            replace_tokenized_values(df_input, token_cache)
        
        assert str(exc_info.value) == "Test exception"
        assert any("Test exception" in message for message in caplog.text)

def test_replace_tokenized_values_with_empty_cache(spark, df_input):
    # Define the schema explicitly
    schema = StructType([
        StructField("account_number", LongType(), True),
        StructField("tokenization", StringType(), True),
        StructField("plain_text", StringType(), True)
    ])
    empty_token_cache = spark.createDataFrame([], schema)
    result_df = replace_tokenized_values(df_input, empty_token_cache)
    assert_dataframe_equality(result_df, df_input)
