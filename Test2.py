


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

def assert_dataframe_equality(df1, df2):
    df1_sorted = df1.sort("account_number", "tokenization")
    df2_sorted = df2.sort("account_number", "tokenization")
    assert df1_sorted.collect() == df2_sorted.collect()

def test_replace_tokenized_values_exception_handling(spark, df_input, token_cache, caplog):
    with patch('your_module.replace_tokenized_values', side_effect=Exception("Test exception")):
        with pytest.raises(Exception) as exc_info:
            replace_tokenized_values(df_input, token_cache)
        
        assert str(exc_info.value) == "Test exception"
        assert "Test exception" in caplog.text

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

###

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

def assert_dataframe_equality(df1, df2):
    df1_sorted = df1.sort("account_number", "tokenization")
    df2_sorted = df2.sort("account_number", "tokenization")
    assert df1_sorted.collect() == df2_sorted.collect()

def test_replace_tokenized_values_exception_handling(spark, df_input, token_cache, caplog):
    with patch('your_module.replace_tokenized_values') as mock_replace_tokenized_values:
        mock_replace_tokenized_values.side_effect = Exception("Test exception")
        
        with pytest.raises(Exception) as exc_info:
            replace_tokenized_values(df_input, token_cache)
        
        assert str(exc_info.value) == "Test exception"
        assert "Test exception" in caplog.text

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


#########


def replace_tokenized_values(input_df, token_cache_df):
    try:
        # Select the necessary columns from token_cache_df
        token_cache_selected_df = token_cache_df.select("account_number", "tokenization", "plain_text").withColumnRenamed(
            "plain_text", "new_formatted"
        )
        
        # Perform the join on 'account_number' and 'tokenization' and select the columns, replacing input_df's value column
        joined_df = input_df.join(token_cache_selected_df, on=["account_number", "tokenization"], how="left")
        
        # Replace the original 'formatted' column with the new 'plain_text' column
        updated_df = joined_df.withColumn(
            "formatted", coalesce(token_cache_selected_df["new_formatted"], input_df["formatted"])
        ).drop("new_formatted")
        
        # Reorder the columns to match the original input_df schema
        original_columns = input_df.columns
        final_df = updated_df.select(original_columns)
        
        return final_df
    except Exception as e:
        logger.error(e)

# Example of the previous test function with renamed variables
def test_replace_tokenized_values_with_empty_cache(spark, input_df):
    # Define the schema explicitly
    schema = StructType([
        StructField("account_number", LongType(), True),
        StructField("tokenization", StringType(), True),
        StructField("plain_text", StringType(), True)
    ])
    empty_token_cache_df = spark.createDataFrame([], schema)
    result_df = replace_tokenized_values(input_df, empty_token_cache_df)
    assert_dataframe_equality(result_df, input_df)
