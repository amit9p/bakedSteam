
from pyspark.sql import functions as F

def calculate_highest_credit(df):
    """
    Calculates the highest credit amount utilized across all accounts and 
    adds it as a new column to the DataFrame along with account_id.
    
    :param df: DataFrame containing account history including 'account_id' and 'credit_utilized'.
    :return: DataFrame with 'account_id' and 'highest_credit' as columns.
    """
    # Calculate the maximum credit utilized across all entries
    max_credit = df.agg(F.max(col('credit_utilized')).alias('highest_credit')).collect()[0]['highest_credit']
    
    # Add 'highest_credit' to each row, keeping 'account_id' intact
    return df.select('account_id', 'credit_utilized').withColumn('highest_credit', F.lit(max_credit))

# Example of usage
# Assuming spark is your SparkSession and df is loaded with appropriate data
# df = spark.createDataFrame([...])
# result_df = calculate_highest_credit(df)
# result_df.show()


import pytest
from pyspark.sql import SparkSession, Row

def test_calculate_highest_credit():
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    test_data = [
        Row(account_id=1, credit_utilized=500),
        Row(account_id=2, credit_utilized=1500),
        Row(account_id=3, credit_utilized=1000)  # Example data
    ]
    df = spark.createDataFrame(test_data)
    result_df = calculate_highest_credit(df)
    expected_data = [
        Row(account_id=1, credit_utilized=500, highest_credit=1500),
        Row(account_id=2, credit_utilized=1500, highest_credit=1500),
        Row(account_id=3, credit_utilized=1000, highest_credit=1500)  # Highest credit should be 1500 for all records
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert result_df.collect() == expected_df.collect()

# Run the test
test_calculate_highest_credit()
