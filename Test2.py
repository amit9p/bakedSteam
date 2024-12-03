
def test_highest_credit_value_negative_credit(spark_session):
    """
    Test the scenario where the highest credit value is negative.
    """
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("highest_credit_value", IntegerType(), True)
    ])
    data = [("1", -100), ("2", 2000)]
    input_df = spark_session.createDataFrame(data, schema)
    output_df = highest_credit_value(input_df)

    # Assert that the output is not filtered incorrectly
    assert output_df.count() == 2  # Check that both rows are present
    
    # Assert specific values
    output_data = output_df.collect()  # Collect data for assertion
    expected_data = [("1", -100), ("2", 2000)]
    actual_data = [(row["account_id"], row["highest_credit_value"]) for row in output_data]
    assert actual_data == expected_data  # Verify that values match


def test_highest_credit_value_missing_column(spark_session):
    """
    Test the scenario where a required column is missing or has None values.
    """
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("highest_credit_value", IntegerType(), True)
    ])
    
    # Include None in the dataset
    data = [("1", None), ("2", 500), ("3", None)]
    input_df = spark_session.createDataFrame(data, schema)
    
    # Call the function and check for the output
    output_df = highest_credit_value(input_df)

    # Assert that rows with None values are handled correctly
    assert output_df.count() == 3  # Ensure all rows are present
    assert output_df.filter(output_df.highest_credit_value.isNull()).count() == 2  # Check the count of None values
    
    # Assert specific values
    output_data = output_df.collect()  # Collect data for assertion
    expected_data = [("1", None), ("2", 500), ("3", None)]
    actual_data = [(row["account_id"], row["highest_credit_value"]) for row in output_data]
    assert actual_data == expected_data  # Verify that values match
