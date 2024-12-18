
print("Expected Data:", expected_df_sorted.collect())
print("Result Data:", result_df_sorted.collect())
def test_calculate_country_code_empty_dataframe(spark_session):
    # Edge case: Empty DataFrame with required columns
    from pyspark.sql.types import StructType, StructField, StringType
    
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("country_code", StringType(), True)
    ])
    
    df = spark_session.createDataFrame([], schema)
    result_df = calculate_country_code(df).collect()

    # Should return an empty DF with the same schema
    assert len(result_df) == 0
    assert "account_id" in calculate_country_code(df).columns
    assert "country_code" in calculate_country_code(df).columns


#####
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def calculate_country_code(input_df: DataFrame) -> DataFrame:
    """
    Returns the country_code for each account_id.

    :param input_df: Input DataFrame with columns:
        - 'account_id': The ID of the account.
        - 'country_code': The country code associated with this account.
    
    :return: Output DataFrame with columns:
        - 'account_id': The ID of the account.
        - 'country_code': The country code associated with the account.
    """
    required_columns = {"account_id", "country_code"}
    if not required_columns.issubset(input_df.columns):
        raise ValueError(f"Input DataFrame must contain the following columns: {required_columns}")

    return input_df.select(
        col("account_id"),
        col("country_code")
    )




import pytest
from pyspark.sql import Row
from transform_module import calculate_country_code  # Adjust as needed

def test_calculate_country_code_positive(spark):
    # Positive test: Both columns present and valid values
    input_data = [
        ("123", "US"),
        ("456", "GBR"),
        ("789", "IND")
    ]
    df = spark.createDataFrame(input_data, ["account_id", "country_code"])
    result_df = calculate_country_code(df).collect()

    assert len(result_df) == 3
    assert result_df[0]["account_id"] == "123" and result_df[0]["country_code"] == "US"
    assert result_df[1]["account_id"] == "456" and result_df[1]["country_code"] == "GBR"
    assert result_df[2]["account_id"] == "789" and result_df[2]["country_code"] == "IND"

def test_calculate_country_code_nullable(spark):
    # Edge case: country_code is nullable
    input_data = [
        ("123", None),
        ("456", "  "),   # just spaces
        ("789", "")
    ]
    df = spark.createDataFrame(input_data, ["account_id", "country_code"])
    result_df = calculate_country_code(df).collect()

    # Check that nullable and unusual strings are passed through unchanged
    assert result_df[0]["account_id"] == "123"
    assert result_df[0]["country_code"] is None

    assert result_df[1]["account_id"] == "456"
    assert result_df[1]["country_code"] == "  "

    assert result_df[2]["account_id"] == "789"
    assert result_df[2]["country_code"] == ""

def test_calculate_country_code_missing_country_code_column(spark):
    # Negative test: Missing 'country_code' column
    input_data = [
        ("123",),
        ("456",)
    ]
    df = spark.createDataFrame(input_data, ["account_id"])

    with pytest.raises(ValueError) as exc_info:
        calculate_country_code(df)
    assert "must contain the following columns" in str(exc_info.value)

def test_calculate_country_code_missing_account_id_column(spark):
    # Negative test: Missing 'account_id' column
    input_data = [
        ("US",),
        ("GBR",)
    ]
    df = spark.createDataFrame(input_data, ["country_code"])

    with pytest.raises(ValueError) as exc_info:
        calculate_country_code(df)
    assert "must contain the following columns" in str(exc_info.value)

def test_calculate_country_code_special_chars(spark):
    # Edge case: Special and non-Latin characters
    input_data = [
        ("123", "@#$!"),
        ("456", "日本"),
        ("789", "México")
    ]
    df = spark.createDataFrame(input_data, ["account_id", "country_code"])
    result_df = calculate_country_code(df).collect()

    assert result_df[0]["country_code"] == "@#$!"
    assert result_df[1]["country_code"] == "日本"
    assert result_df[2]["country_code"] == "México"

def test_calculate_country_code_empty_dataframe(spark):
    # Edge case: Empty DataFrame with required columns
    df = spark.createDataFrame([], ["account_id", "country_code"])
    result_df = calculate_country_code(df).collect()

    # Should return an empty DF with the same schema
    assert len(result_df) == 0
    assert "account_id" in calculate_country_code(df).columns
    assert "country_code" in calculate_country_code(df).columns
