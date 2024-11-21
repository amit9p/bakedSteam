
from pyspark.sql.functions import col

def calculate_highest_credit(df):
    """
    Extracts the highest credit amount utilized from the DataFrame.
    
    :param df: DataFrame containing account history.
    :return: DataFrame with highest credit utilized.
    """
    # Assuming 'credit_utilized' is the column storing credit amount used by the consumer at different points.
    return df.withColumn('highest_credit', max(col('credit_utilized')))



import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row

def test_calculate_highest_credit():
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    test_data = [
        Row(credit_utilized=500),
        Row(credit_utilized=1500),
        Row(credit_utilized=1000)  # Example data
    ]
    df = spark.createDataFrame(test_data)
    result_df = calculate_highest_credit(df)
    expected_data = [
        Row(credit_utilized=500, highest_credit=1500),
        Row(credit_utilized=1500, highest_credit=1500),
        Row(credit_utilized=1000, highest_credit=1500)  # Expected highest credit shown for all records
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert result_df.collect() == expected_df.collect()

# Run the test
test_calculate_highest_credit()
