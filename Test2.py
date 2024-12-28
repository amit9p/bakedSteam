
# test_payment_rating_spark.py

import pytest
from pyspark.sql import SparkSession

# Import your updated function (adjust the import path as needed)
from payment_rating_spark import calculate_payment_rating_spark

@pytest.fixture(scope="session")
def spark_session():
    """
    Create or retrieve a Spark session for all tests in this file.
    """
    spark = (
        SparkSession.builder
        # If you do NOT want to specify a master explicitly, just omit .master(...)
        .master("local[1]")
        .appName("TestCalculatePaymentRating")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_schema_output(spark_session):
    """
    Verify that the resulting DataFrame contains exactly:
      [account_id, account_status, past_due_bucket, payment_rating]
    """
    data = [
        (1001, 13, 0),
        (1002, 'DA', 2),
    ]
    columns = ["account_id", "account_status", "past_due_bucket"]
    df_in = spark_session.createDataFrame(data, columns)

    df_out = calculate_payment_rating_spark(df_in)

    expected_cols = ["account_id", "account_status", "past_due_bucket", "payment_rating"]
    assert df_out.columns == expected_cols, (
        f"Expected columns {expected_cols}, but got {df_out.columns}"
    )


def test_blank_account_status(spark_session):
    """
    Test that statuses in [11,78,80,82,83,84,97,64,'DA']
    return a blank string for payment_rating.
    """
    blank_statuses = [11, 78, 80, 82, 83, 84, 97, 64, 'DA']
    data = []
    for i, status in enumerate(blank_statuses, start=1):
        # We'll just set account_id=i, past_due_bucket=0 for simplicity
        data.append((i, status, 0))
    columns = ["account_id", "account_status", "past_due_bucket"]
    df_in = spark_session.createDataFrame(data, columns)

    df_out = calculate_payment_rating_spark(df_in)
    out_rows = df_out.collect()

    for row in out_rows:
        assert row.payment_rating == "", (
            f"Expected blank string, got '{row.payment_rating}' for status={row.account_status}"
        )


def test_account_status_13_valid_past_due_buckets(spark_session):
    """
    Test that account_status=13 with valid bucket values 0..6
    returns '0','1','2','3','4','5','L' respectively.
    """
    data = []
    # We'll create 7 rows for buckets 0..6
    for b in range(7):
        # Keep account_id distinct: 2000+b
        data.append((2000 + b, 13, b))
    columns = ["account_id", "account_status", "past_due_bucket"]
    df_in = spark_session.createDataFrame(data, columns)

    df_out = calculate_payment_rating_spark(df_in).orderBy("past_due_bucket")
    results = df_out.collect()

    expected_map = {
        0: "0",
        1: "1",
        2: "2",
        3: "3",
        4: "4",
        5: "5",
        6: "L"
    }
    for row in results:
        bucket = row.past_due_bucket
        expected_rating = expected_map[bucket]
        assert row.payment_rating == expected_rating, (
            f"For bucket={bucket}, expected '{expected_rating}' but got '{row.payment_rating}'"
        )


def test_account_status_13_invalid_past_due_bucket(spark_session):
    """
    Test that account_status=13 with invalid buckets (negative or >6)
    returns payment_rating=None.
    """
    data = [
        (3001, 13, -1),
        (3002, 13, 7),
        (3003, 13, 999)
    ]
    columns = ["account_id", "account_status", "past_due_bucket"]
    df_in = spark_session.createDataFrame(data, columns)

    df_out = calculate_payment_rating_spark(df_in)
    rows = df_out.collect()

    for row in rows:
        assert row.payment_rating is None, (
            f"Expected None for bucket={row.past_due_bucket}, got '{row.payment_rating}'"
        )


def test_unexpected_account_status(spark_session):
    """
    Test that an account_status not in [11,78,80,82,83,84,97,64,'DA',13]
    yields payment_rating=None.
    """
    data = [
        (4001, 999, 0),
        (4002, 10,  1),
        (4003, "XYZ", 2)
    ]
    columns = ["account_id", "account_status", "past_due_bucket"]
    df_in = spark_session.createDataFrame(data, columns)

    df_out = calculate_payment_rating_spark(df_in)
    rows = df_out.collect()

    for row in rows:
        assert row.payment_rating is None, (
            f"Expected None for account_status={row.account_status}, got '{row.payment_rating}'"
        )


def test_mixed_values(spark_session):
    """
    Test a single DataFrame with mixed account_status and bucket values.
    """
    data = [
        # account_id, account_status, past_due_bucket
        (5001, 13,    2),   # => '2'
        (5002, 78,    1),   # => ''
        (5003, 13,    4),   # => '4'
        (5004, 'DA',  6),   # => ''
        (5005, 13,    6),   # => 'L'
        (5006, 64,    0),   # => ''
        (5007, 13,    9),   # => None
        (5008, 999,   2),   # => None (unexpected status)
    ]
    columns = ["account_id", "account_status", "past_due_bucket"]
    df_in = spark_session.createDataFrame(data, columns)

    df_out = calculate_payment_rating_spark(df_in).orderBy("account_id")
    rows = df_out.collect()

    # We'll map out each row's expected payment_rating
    expected_by_id = {
        5001: "2",
        5002: "",
        5003: "4",
        5004: "",
        5005: "L",
        5006: "",
        5007: None,
        5008: None
    }

    for row in rows:
        acct_id = row.account_id
        expected = expected_by_id[acct_id]
        assert row.payment_rating == expected, (
            f"For account_id={acct_id}, expected '{expected}' but got '{row.payment_rating}'"
        )

_______________
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

def calculate_payment_rating_spark(input_df: DataFrame) -> DataFrame:
    """
    Given a PySpark DataFrame containing 'account_id', 'account_status',
    and 'past_due_bucket', compute 'payment_rating' using the
    if/then logic from your spec, and return a new DataFrame
    with at least 'account_id' and 'payment_rating'.
    """

    blank_statuses = [11, 78, 80, 82, 83, 84, 97, 64, 'DA']

    payment_rating_col = (
        when(col("account_status").isin(blank_statuses), lit(""))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 0), lit("0"))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 1), lit("1"))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 2), lit("2"))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 3), lit("3"))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 4), lit("4"))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 5), lit("5"))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 6), lit("L"))
        .otherwise(lit(None))
    )

    # Add the payment_rating column
    df_with_rating = input_df.withColumn("payment_rating", payment_rating_col)

    # Option A: Return all columns (including account_id)
    # return df_with_rating

    # Option B: Return *only* account_id + payment_rating + whichever columns you want
    return df_with_rating.select(
        "account_id",
        "account_status",
        "past_due_bucket",
        "payment_rating"
    )



###########


from pyspark.sql import SparkSession

def create_spark_session():
    spark = (
        SparkSession
        .builder
        .appName("My App")
        .getOrCreate()
    )
    return spark

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

def calculate_payment_rating_spark(input_df: DataFrame) -> DataFrame:
    """
    Given a PySpark DataFrame containing 'account_status' and 'past_due_bucket',
    compute 'payment_rating' using the if/then logic from your spec.

    Rules Recap:
      - If account_status in [11, 78, 80, 82, 83, 84, 97, 64, 'DA']:
          payment_rating = ''  (blank string)
      - Else if account_status = 13:
          past_due_bucket = 0 -> payment_rating = '0'
          past_due_bucket = 1 -> payment_rating = '1'
          past_due_bucket = 2 -> payment_rating = '2'
          past_due_bucket = 3 -> payment_rating = '3'
          past_due_bucket = 4 -> payment_rating = '4'
          past_due_bucket = 5 -> payment_rating = '5'
          past_due_bucket = 6 -> payment_rating = 'L'
          otherwise -> None
      - Otherwise (account_status not in the above sets),
          payment_rating = None
    """

    blank_statuses = [11, 78, 80, 82, 83, 84, 97, 64, 'DA']

    # Construct the payment_rating column using a chain of when() conditions:
    payment_rating_col = (
        # 1) If in blank_statuses => '' (blank string)
        when(col("account_status").isin(blank_statuses), lit(""))
        
        # 2) If account_status == 13, then handle past_due_bucket logic
        .when((col("account_status") == 13) & (col("past_due_bucket") == 0), lit("0"))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 1), lit("1"))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 2), lit("2"))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 3), lit("3"))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 4), lit("4"))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 5), lit("5"))
        .when((col("account_status") == 13) & (col("past_due_bucket") == 6), lit("L"))

        # 3) Otherwise => None
        .otherwise(lit(None))
    )

    # Return a new DataFrame with the derived column
    return input_df.withColumn("payment_rating", payment_rating_col)



# test_payment_rating_spark.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Import the function we want to test
from payment_rating_spark import calculate_payment_rating_spark

@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for testing.
    """
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("TestCalculatePaymentRating") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_blank_account_status(spark_session):
    """
    Test statuses in [11, 78, 80, 82, 83, 84, 97, 64, 'DA']
    return a blank string for payment_rating.
    """
    blank_statuses = [11, 78, 80, 82, 83, 84, 97, 64, 'DA']
    data = [(status, 0) for status in blank_statuses]
    columns = ["account_status", "past_due_bucket"]
    df_in = spark_session.createDataFrame(data, columns)

    df_out = calculate_payment_rating_spark(df_in)
    results = df_out.select("payment_rating").rdd.flatMap(lambda x: x).collect()

    for r in results:
        assert r == "", f"Expected blank string, got {r}."


def test_account_status_13_valid_past_due_buckets(spark_session):
    """
    Test that account_status = 13 with valid bucket values
    returns the correct payment ratings.
    """
    # We'll test buckets 0..6
    data = [(13, i) for i in range(7)]
    columns = ["account_status", "past_due_bucket"]
    df_in = spark_session.createDataFrame(data, columns)

    df_out = calculate_payment_rating_spark(df_in)
    results = df_out.select("past_due_bucket", "payment_rating") \
                    .orderBy("past_due_bucket") \
                    .collect()

    expected_map = {
        0: "0",
        1: "1",
        2: "2",
        3: "3",
        4: "4",
        5: "5",
        6: "L"
    }

    for row in results:
        bucket = row["past_due_bucket"]
        actual = row["payment_rating"]
        expected = expected_map[bucket]
        assert actual == expected, f"For bucket {bucket}, expected {expected} but got {actual}"


def test_account_status_13_invalid_past_due_bucket(spark_session):
    """
    Test that account_status=13 with an invalid past-due bucket
    (negative or beyond 6) returns None.
    """
    data = [(13, -1), (13, 7), (13, 999)]
    columns = ["account_status", "past_due_bucket"]
    df_in = spark_session.createDataFrame(data, columns)

    df_out = calculate_payment_rating_spark(df_in)
    results = df_out.select("payment_rating").rdd.flatMap(lambda x: x).collect()

    for r in results:
        assert r is None, f"Expected None for invalid buckets, got {r}."


def test_unexpected_account_status(spark_session):
    """
    Test that an account_status not in [11,78,80,82,83,84,97,64,'DA',13]
    yields payment_rating = None.
    """
    data = [(999, 0), (10, 1), ("XYZ", 2)]
    columns = ["account_status", "past_due_bucket"]
    df_in = spark_session.createDataFrame(data, columns)

    df_out = calculate_payment_rating_spark(df_in)
    results = df_out.select("payment_rating").rdd.flatMap(lambda x: x).collect()

    for r in results:
        assert r is None, f"Expected None for unexpected statuses, got {r}."


def test_mixed_values(spark_session):
    """
    Test mixing various account_status and bucket values in one DataFrame.
    """
    data = [
        (13, 2),    # => '2'
        (78, 1),    # => ''
        (13, 4),    # => '4'
        ('DA', 6),  # => ''
        (13, 6),    # => 'L'
        (64, 0)     # => ''
    ]
    columns = ["account_status", "past_due_bucket"]
    df_in = spark_session.createDataFrame(data, columns)

    df_out = calculate_payment_rating_spark(df_in).orderBy("past_due_bucket")

    # Collect the result in the same order we inserted, so let's just do a .collect() 
    # but make sure we don't rely on any reorder unless we explicitly do .orderBy. 
    # We'll just gather them in the same sequence for clarity:
    df_out_unordered = calculate_payment_rating_spark(df_in)
    results = df_out_unordered.select("payment_rating").rdd.flatMap(lambda x: x).collect()

    # Expected in the same row order:
    # (13, 2) => "2"
    # (78, 1) => ""
    # (13, 4) => "4"
    # ('DA',6) => ""
    # (13, 6) => "L"
    # (64, 0) => ""
    expected = ["2", "", "4", "", "L", ""]

    assert len(results) == len(expected), "Mismatch in the number of returned rows"
    for i in range(len(results)):
        assert results[i] == expected[i], f"Row {i} => expected {expected[i]} but got {results[i]}"
