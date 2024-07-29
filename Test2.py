
import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch
from your_module import replace_tokenized_values  # Replace with your actual module name

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

def test_replace_tokenized_values_exception_handling(spark, df_input, token_cache):
    with patch('your_module.replace_tokenized_values') as mock_replace_tokenized_values:
        mock_replace_tokenized_values.side_effect = Exception("Test exception")
        try:
            replace_tokenized_values(df_input, token_cache)
        except Exception as e:
            assert str(e) == "Test exception"

# Additional test cases for better coverage
def assert_dataframe_equality(df1, df2):
    df1_sorted = df1.sort(col("account_number"), col("tokenization"))
    df2_sorted = df2.sort(col("account_number"), col("tokenization"))
    assert df1_sorted.collect() == df2_sorted.collect()

def test_replace_tokenized_values_with_empty_input(spark, token_cache):
    schema = StructType([
        StructField("account_number", LongType(), True),
        StructField("tokenization", StringType(), True),
        StructField("formatted", StringType(), True)
    ])
    empty_df_input = spark.createDataFrame([], schema)
    result_df = replace_tokenized_values(empty_df_input, token_cache)
    assert_dataframe_equality(result_df, empty_df_input)

def test_replace_tokenized_values(spark, df_input, token_cache):
    result_df = replace_tokenized_values(df_input, token_cache)
    expected_data = [
        (1, "token_1", "plain_text_1"),
        (2, "token_2", "plain_text_2"),
        (3, "token_3", "plain_text_3")
    ]
    expected_schema = ["account_number", "tokenization", "formatted"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert_dataframe_equality(result_df, expected_df)

def test_replace_tokenized_values_with_missing_keys(spark, df_input, token_cache):
    modified_token_cache = token_cache.filter("account_number != 1")
    result_df = replace_tokenized_values(df_input, modified_token_cache)
    expected_data = [
        (1, "token_1", "formatted_1"),  # Original value should remain
        (2, "token_2", "plain_text_2"),
        (3, "token_3", "plain_text_3")
    ]
    expected_schema = ["account_number", "tokenization", "formatted"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    assert_dataframe_equality(result_df, expected_df)

def test_replace_tokenized_values_with_empty_cache(spark, df_input):
    empty_token_cache = spark.createDataFrame([], ["account_number", "tokenization", "plain_text"])
    result_df = replace_tokenized_values(df_input, empty_token_cache)
    assert_dataframe_equality(result_df, df_input)
