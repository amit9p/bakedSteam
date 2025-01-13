

Yes, the updated test cases completely avoid the use of orderBy and for loops, fulfilling the review comments:

1. No orderBy:

Instead of relying on row order, the test cases use assert_dataframe_equality, which compares the DataFrames based on their contents regardless of row order.



2. No for loops:

The test cases directly compare the entire DataFrames using assert_dataframe_equality. There's no need to iterate through rows.




from pyspark.sql.types import StructType, StructField, StringType

def test_unrecognized_portfolio_type(spark_session):
    data = [("A001", "X", "100"), ("A002", "Y", "200")]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit")

    # Define explicit schema for the expected DataFrame
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("calculated_credit_limit", StringType(), True)
    ])
    expected_data = [("A001", None), ("A002", None)]
    expected_df = spark_session.createDataFrame(expected_data, schema=schema)

    assert_dataframe_equality(result, expected_df)


def test_invalid_assigned_value(spark_session):
    data = [("A001", "R", "abc"), ("A002", "R", "---")]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit")

    # Define explicit schema for the expected DataFrame
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("calculated_credit_limit", StringType(), True)
    ])
    expected_data = [("A001", None), ("A002", None)]
    expected_df = spark_session.createDataFrame(expected_data, schema=schema)

    assert_dataframe_equality(result, expected_df)




Yes, the updated test cases remove both orderBy and for loops completely, addressing the review comments:

1. orderBy Removed:

Instead of relying on orderBy, the test cases compare the results using a set-based comparison (set([tuple(row) for row in df.collect()])), which ensures the order of rows is irrelevant.



2. for Loops Removed:

The test cases now directly assert equality of entire DataFrames using the assert_dataframe_equality helper function, avoiding the need to iterate through rows using for loops.




def assert_dataframe_equality(df1, df2):
    """
    Compares two DataFrames for equality by converting them to sets of rows.
    """
    assert set([tuple(row) for row in df1.collect()]) == set([tuple(row) for row in df2.collect()]), "DataFrames are not equal"


def test_portfolio_o_zero_filled(spark_session):
    data = [("A001", "O", "500"), ("A002", "O", "999")]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit")
    expected_data = [("A001", "0"), ("A002", "0")]
    expected_df = spark_session.createDataFrame(expected_data, ["account_id", "calculated_credit_limit"])

    assert_dataframe_equality(result, expected_df)


def test_portfolio_r_numeric_limit(spark_session):
    data = [("A001", "R", "129.2"), ("A002", "R", "456.7"), ("A003", "R", "123.5")]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit")
    expected_data = [("A001", "129"), ("A002", "457"), ("A003", "124")]
    expected_df = spark_session.createDataFrame(expected_data, ["account_id", "calculated_credit_limit"])

    assert_dataframe_equality(result, expected_df)


def test_portfolio_r_npsl(spark_session):
    data = [("A001", "R", "NPSL"), ("A002", "R", "npsl")]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit")
    expected_data = [("A001", "NPSL"), ("A002", None)]
    expected_df = spark_session.createDataFrame(expected_data, ["account_id", "calculated_credit_limit"])

    assert_dataframe_equality(result, expected_df)


def test_unrecognized_portfolio_type(spark_session):
    data = [("A001", "X", "100"), ("A002", "Y", "200")]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit")
    expected_data = [("A001", None), ("A002", None)]
    expected_df = spark_session.createDataFrame(expected_data, ["account_id", "calculated_credit_limit"])

    assert_dataframe_equality(result, expected_df)


def test_invalid_assigned_value(spark_session):
    data = [("A001", "R", "abc"), ("A002", "R", "---")]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit")
    expected_data = [("A001", None), ("A002", None)]
    expected_df = spark_session.createDataFrame(expected_data, ["account_id", "calculated_credit_limit"])

    assert_dataframe_equality(result, expected_df)


def test_mixed_values(spark_session):
    data = [
        ("A001", "O", "500"),
        ("A002", "R", "129.5"),
        ("A003", "R", "NPSL"),
        ("A004", "X", "100"),
        ("A005", "R", "invalid"),
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit")
    expected_data = [
        ("A001", "0"),
        ("A002", "130"),
        ("A003", "NPSL"),
        ("A004", None),
        ("A005", None),
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["account_id", "calculated_credit_limit"])

    assert_dataframe_equality(result, expected_df)
