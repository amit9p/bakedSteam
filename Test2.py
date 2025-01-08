
We only sort to ensure a stable, consistent row order for comparison. We’re not testing the row order itself; we verify row counts and contents match our expectations.

    ###

In Spark, DataFrames have no guaranteed inherent row ordering. When you do something like:

df_out = calculate_credit_limit_spark(df_in).collect()

the rows can come back in any sequence (depending on optimizations or distributed processing). If your test expects the rows in a particular order (e.g. matching index-by-index with your “expected” list), you can get spurious test failures if rows are returned in a different sequence.

By adding:

df_out = calculate_credit_limit_spark(df_in).orderBy("account_id").collect()

you ensure deterministic ordering by account_id before collecting the results. This way, you always know which row in the collected list corresponds to which account, and your test’s row-by-row comparisons will be stable and consistent every time you run it.




from pyspark.sql.functions import col, when, lit, round as spark_round

parsed_assigned_limit = when(
    col("assigned_credit_limit") == "NPSL", 
    lit("NPSL")
).otherwise(
    spark_round(col("assigned_credit_limit").cast("double")).cast("long").cast("string")
)

final_credit_limit = (
    when(col("portfolio_type") == "O", lit("0"))
    .when(col("portfolio_type") == "R", parsed_assigned_limit)
    .otherwise(lit(None).cast("string"))
)

#####

data = [
    ("A001", "O", 500.0),    # Use 500.0 instead of 500
    ("A002", "R", 129.5),
    ("A003", "R", "NPSL"),
    ("A004", "X", "100"),
    ("A005", "R", "invalid"),
]
cols = ["account_id", "portfolio_type", "assigned_credit_limit"]

df_in = spark_session.createDataFrame(data, cols)
######


from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, round as spark_round

def calculate_credit_limit_spark(input_df: DataFrame) -> DataFrame:
    """
    Given a PySpark DataFrame with columns:
      - account_id (any type, retained in output)
      - portfolio_type (string; valid values 'O' or 'R', could be others)
      - assigned_credit_limit (int/float/string; might be 'NPSL' or a numeric value)
    Return a new DataFrame with:
      - account_id
      - calculated_credit_limit

    Logic:
      1. If portfolio_type = 'O':
         --> calculated_credit_limit = 0 (zero-filled)
      2. If portfolio_type = 'R':
         --> if assigned_credit_limit = 'NPSL', keep 'NPSL'
         --> else parse assigned_credit_limit as float & round to nearest whole dollar
      3. Otherwise, set calculated_credit_limit = None
    """

    # Handle the assigned credit limit:
    # If it's 'NPSL', keep 'NPSL'. Else, convert to float & round.
    parsed_assigned_limit = when(
        col("assigned_credit_limit") == "NPSL",
        lit("NPSL")
    ).otherwise(
        spark_round(col("assigned_credit_limit").cast("double"))
    )

    # Now apply the logic based on portfolio_type.
    final_credit_limit = (
        when(col("portfolio_type") == lit("O"), lit(0))
        .when(col("portfolio_type") == lit("R"), parsed_assigned_limit)
        .otherwise(lit(None))
    )

    # Return a DF with just the account_id & calculated_credit_limit columns
    return (
        input_df
        .withColumn("calculated_credit_limit", final_credit_limit)
        .select("account_id", "calculated_credit_limit")
    )



########

import pytest
from pyspark.sql import SparkSession, Row
from credit_limit_calc import calculate_credit_limit_spark

@pytest.fixture(scope="session")
def spark_session():
    """
    Create a local SparkSession for testing.
    """
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("TestCreditLimitCalculation")
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_portfolio_o_zero_filled(spark_session):
    """
    If portfolio_type = 'O', the calculated credit limit should be 0.
    """
    data = [
        ("A001", "O", 500),
        ("A002", "O", "999"),
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    df_out = calculate_credit_limit_spark(df_in).orderBy("account_id").collect()

    # Check results
    assert df_out[0]["account_id"] == "A001"
    assert df_out[0]["calculated_credit_limit"] == 0

    assert df_out[1]["account_id"] == "A002"
    assert df_out[1]["calculated_credit_limit"] == 0

def test_portfolio_r_numeric_limit(spark_session):
    """
    If portfolio_type = 'R' and assigned_credit_limit is numeric,
    we should round to the nearest whole dollar.
    """
    data = [
        ("A001", "R", 129.2),
        ("A002", "R", "456.7"),
        ("A003", "R", 123.5),
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)
    df_out = calculate_credit_limit_spark(df_in).orderBy("account_id").collect()

    # A001 => 129.2 => round => 129
    assert df_out[0]["calculated_credit_limit"] == 129
    # A002 => "456.7" => cast to float => round => 457
    assert df_out[1]["calculated_credit_limit"] == 457
    # A003 => 123.5 => should round to 124
    assert df_out[2]["calculated_credit_limit"] == 124

def test_portfolio_r_npsl(spark_session):
    """
    If portfolio_type = 'R' and assigned_credit_limit = 'NPSL',
    the result should remain 'NPSL'.
    """
    data = [
        ("A001", "R", "NPSL"),
        ("A002", "R", "npsl"),  # see if mismatch case
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    # We'll assume the function only checks uppercase 'NPSL';
    # so 'npsl' might cause a cast failure => None, or we adapt logic if needed
    # For demonstration, let's see what it actually does.

    df_out = calculate_credit_limit_spark(df_in).orderBy("account_id").collect()

    # A001 => "NPSL" => 'NPSL'
    assert df_out[0]["calculated_credit_limit"] == "NPSL"

    # A002 => "npsl" => likely tries casting => fails => null (based on current code)
    # If we want to handle case-insensitively, we'd adjust the function logic.
    assert df_out[1]["calculated_credit_limit"] is None

def test_unrecognized_portfolio_type(spark_session):
    """
    If portfolio_type is not 'O' or 'R', we expect None for calculated_credit_limit.
    """
    data = [
        ("A001", "X", 100),
        ("A002", "", 200),
        ("A003", None, 300),
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)
    df_out = calculate_credit_limit_spark(df_in).orderBy("account_id").collect()

    for row in df_out:
        assert row["calculated_credit_limit"] is None

def test_invalid_assigned_value(spark_session):
    """
    If assigned_credit_limit is not numeric or 'NPSL', it casts to float => fails => returns None.
    """
    data = [
        ("A001", "R", "abc"),
        ("A002", "R", "---"),
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]

    df_in = spark_session.createDataFrame(data, cols)
    df_out = calculate_credit_limit_spark(df_in).orderBy("account_id").collect()

    for row in df_out:
        assert row["calculated_credit_limit"] is None

def test_mixed_values(spark_session):
    """
    Test a single DataFrame with a variety of scenarios.
    """
    data = [
        ("A001", "O", 500),       # => 0
        ("A002", "R", 129.5),     # => round(129.5) => 130
        ("A003", "R", "NPSL"),    # => 'NPSL'
        ("A004", "X", "100"),     # => None
        ("A005", "R", "invalid"), # => None
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]

    df_in = spark_session.createDataFrame(data, cols)
    df_out = calculate_credit_limit_spark(df_in).orderBy("account_id").collect()

    expected_map = {
        "A001": 0,
        "A002": 130,
        "A003": "NPSL",
        "A004": None,
        "A005": None
    }

    for row in df_out:
        acct_id = row["account_id"]
        assert row["calculated_credit_limit"] == expected_map[acct_id], (
            f"For {acct_id}, expected {expected_map[acct_id]}, got {row['calculated_credit_limit']}"
        )
