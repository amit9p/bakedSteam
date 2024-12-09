

import uuid

def lambda_handler(event, context):
    run_id = event.get("run_id", str(uuid.uuid4()))
    print(f"Run ID: {run_id}")
    return run_id


def test_date_of_last_payment_with_none_account_id(spark_session):
    # Define schema
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
    ])

    # Input data (account_id includes None)
    data = [
        (None, "2024-12-01"),  # Null account_id
        ("1", "2024-11-25"),
        ("2", "2024-11-20"),
        ("2", "2024-12-05"),  # Latest transaction for account_id "2"
    ]
    input_df = spark_session.createDataFrame(data, schema)

    # Expected output
    expected_data = [
        (None, "2024-12-01"),  # Null account_id is retained
        ("1", "2024-11-25"),
        ("2", "2024-12-05"),  # Latest transaction for account_id "2"
    ]
    expected_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("date_of_last_payment", StringType(), True),
    ])
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    # Call the method
    result_df = date_of_last_payment(input_df)

    # Custom sorting key to handle None values in account_id
    def sort_key(row):
        return (row["account_id"] is None, row["account_id"], row["date_of_last_payment"])

    # Assertions
    assert result_df.count() == expected_df.count(), "Row counts do not match"
    assert sorted(result_df.collect(), key=sort_key) == sorted(expected_df.collect(), key=sort_key), "Output data does not match expected data"


#####
def test_date_of_last_payment_with_none_account_id(spark_session):
    # Define schema
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
    ])

    # Input data (account_id includes None)
    data = [
        (None, "2024-12-01"),  # Null account_id
        ("1", "2024-11-25"),
        ("2", "2024-11-20"),
        ("2", "2024-12-05"),  # Latest transaction for account_id "2"
    ]
    input_df = spark_session.createDataFrame(data, schema)

    # Expected output
    expected_data = [
        (None, "2024-12-01"),  # Null account_id is retained
        ("1", "2024-11-25"),
        ("2", "2024-12-05"),  # Latest transaction for account_id "2"
    ]
    expected_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("date_of_last_payment", StringType(), True),
    ])
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    # Call the method
    result_df = date_of_last_payment(input_df)

    # Assertions
    assert result_df.count() == expected_df.count(), "Row counts do not match"
    assert sorted(result_df.collect()) == sorted(expected_df.collect()), "Output data does not match expected data"





from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max as spark_max

def date_of_last_payment(input_df: DataFrame) -> DataFrame:
    """
    Returns the date of the last payment for each account_id.
    
    :param input_df: Input DataFrame with columns:
        - 'account_id': The ID of the account.
        - 'transaction_date': The date of the last non-returned payment.
    :return: Output DataFrame with columns:
        - 'account_id': The ID of the account.
        - 'date_of_last_payment': The date of the last payment.
    """
    # Ensure input DataFrame has the required columns
    required_columns = {"account_id", "transaction_date"}
    if not required_columns.issubset(input_df.columns):
        raise ValueError(f"Input DataFrame must contain the following columns: {required_columns}")
    
    # Group by account_id and get the maximum transaction_date
    result_df = input_df.groupBy("account_id").agg(
        spark_max("transaction_date").alias("date_of_last_payment")
    )
    
    return result_df

#####





def date_of_last_payment(input_df: DataFrame) -> DataFrame:
    """
    Returns the date of the last payment for each account_id.
    
    :param input_df: Input DataFrame with columns:
        - 'account_id': The ID of the account.
        - 'transaction_date': The date of the last non-returned payment.
    :return: Output DataFrame with columns:
        - 'account_id': The ID of the account.
        - 'date_of_last_payment': The date of the last payment (backdated transaction date).
    """
    # Ensure input DataFrame has the required columns
    required_columns = {"account_id", "transaction_date"}
    if not required_columns.issubset(input_df.columns):
        raise ValueError(f"Input DataFrame must contain the following columns: {required_columns}")

    # Ensure there are no None values in the 'account_id' column
    if input_df.filter(col("account_id").isNull()).count() > 0:
        raise ValueError("Input DataFrame must contain valid account_id values")
    
    # Select only the relevant columns and rename transaction_date to date_of_last_payment
    result_df = input_df.select(
        col("account_id"),
        col("transaction_date").alias("date_of_last_payment")
    )
    
    return result_df


def test_date_of_last_payment_valid(spark_session):
    # Define schema
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("transaction_date", StringType(), True),  # Date in 'YYYY-MM-DD' format
    ])

    # Input data
    data = [
        ("1", "2024-12-01"),
        ("1", "2024-11-25"),
        ("2", "2024-10-15"),
        ("2", "2024-09-30"),
        ("3", "2024-08-01"),
    ]
    input_df = spark_session.createDataFrame(data, schema)

    # Expected output
    expected_data = [
        ("1", "2024-12-01"),
        ("2", "2024-10-15"),
        ("3", "2024-08-01"),
    ]
    expected_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("date_of_last_payment", StringType(), True),
    ])
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    # Call the method
    result_df = date_of_last_payment(input_df)

    # Assertions
    assert result_df.count() == expected_df.count(), "Row counts do not match"
    assert sorted(result_df.collect()) == sorted(expected_df.collect()), "Output data does not match expected data"

def test_date_of_last_payment_empty(spark_session):
    # Define schema
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
    ])

    # Input data (empty DataFrame)
    data = []
    input_df = spark_session.createDataFrame(data, schema)

    # Expected output (empty DataFrame)
    expected_data = []
    expected_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("date_of_last_payment", StringType(), True),
    ])
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    # Call the method
    result_df = date_of_last_payment(input_df)

    # Assertions
    assert result_df.count() == expected_df.count(), "Row counts do not match for empty DataFrame"
    assert result_df.collect() == expected_df.collect(), "Output does not match expected for empty DataFrame"


def test_date_of_last_payment_missing_columns(spark_session):
    # Define schema
    schema = StructType([
        StructField("account_id", StringType(), True),
    ])

    # Input data (missing 'transaction_date')
    data = [("1",)]
    input_df = spark_session.createDataFrame(data, schema)

    # Call the method and expect a failure
    with pytest.raises(ValueError, match="Input DataFrame must contain the following columns"):
        date_of_last_payment(input_df)


def test_date_of_last_payment_with_none_account_id(spark_session):
    # Define schema
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
    ])

    # Input data (account_id is None)
    data = [
        (None, "2024-12-01"),
    ]
    input_df = spark_session.createDataFrame(data, schema)

    # Call the method and expect a failure
    with pytest.raises(ValueError, match="Input DataFrame must contain valid account_id values"):
        date_of_last_payment(input_df)


def test_date_of_last_payment_multiple_dates(spark_session):
    # Define schema
    schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
    ])

    # Input data (multiple records for the same account_id)
    data = [
        ("1", "2024-11-30"),
        ("1", "2024-12-01"),  # Latest transaction
        ("2", "2024-10-10"),
        ("2", "2024-10-15"),  # Latest transaction
    ]
    input_df = spark_session.createDataFrame(data, schema)

    # Expected output
    expected_data = [
        ("1", "2024-12-01"),
        ("2", "2024-10-15"),
    ]
    expected_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("date_of_last_payment", StringType(), True),
    ])
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    # Call the method
    result_df = date_of_last_payment(input_df)

    # Assertions
    assert result_df.count() == expected_df.count(), "Row counts do not match"
    assert sorted(result_df.collect()) == sorted(expected_df.collect()), "Output data does not match expected data"


1. Valid Input with Non-Empty DataFrame: The method correctly groups by account_id and retrieves the maximum transaction_date for each account.


2. Empty DataFrame Input: The method handles an empty DataFrame gracefully and returns an empty DataFrame as expected.


3. Missing Required Columns: The method raises a ValueError if the required columns (account_id and transaction_date) are missing, as per the implementation.


4. Handling None in account_id: The method raises a ValueError when account_id contains None, ensuring only valid data is processed.


5. Non-Unique Account IDs with Latest Dates: The method correctly identifies the latest transaction_date for each account_id when multiple records exist.






