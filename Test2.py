

is_reactivation_notification_exists = col("reactivation_notification").isNotNull() & (col("reactivation_notification") == 1)
is_deceased_notification_exists = col("deceased_notification").isNotNull() & (col("deceased_notification") == 1)
is_deceased_in_error_notification_exists = col("deceased_in_error_notification").isNotNull() & (col("deceased_in_error_notification") == 1)

=====




Review Comments:

1. Trailing 24 Months Logic:

The implementation does not consider extracting only the last 24 months of payment history from the past_due_bucket column, as described in the output requirements. This logic needs to be explicitly implemented.



2. Character Mapping:

The implementation does not translate the past_due_bucket values into the required characters (e.g., 0 for 0-29 days, 1 for 30-59 days, etc.). This mapping is a key requirement.



3. Handling Bankruptcy Status:

There is no logic for incorporating the Bankruptcy Status field. As per the requirements, if the bankruptcy status is "OPEN," you should add D until it transitions to "CLOSED," "DISCHARGED," or "DISMISSED."



4. Account Open Date:

The implementation does not include handling for B to indicate the account's opening month. This condition is critical if the account was opened within the 24-month window.



5. Charge-Off Handling:

There is no logic for marking L for months when the account is charged off, as specified in the notes.



6. Position Logic:

The implementation lacks the mechanism to ensure the most recent month is at position 127 and the oldest month at position 150 in the output string.



7. Output Formatting:

The result must be a string of 24 characters, representing the payment history, which is not evident in the current implementation.



8. Lagging One Month:

The function does not incorporate the "trailing one month" logic. For example, a March report should include data up to February.



9. Error Handling:

There is no error handling for invalid or missing data in the inputs (e.g., past_due_bucket or Bankruptcy Status).



____<<<<<<<<<________
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Constants for column names
ACCOUNT_ID_COL = "Account ID"
BANKRUPTCY_CHAPTER_COL = "Bankruptcy Chapter"
BANKRUPTCY_STATUS_COL = "Bankruptcy Status"
CONSUMER_INFORMATION_COL = "Consumer Information Indicator"

# Constants for status and chapters
STATUS_OPEN = "Open"
STATUS_DISCHARGED = "Discharge"
STATUS_CLOSED = "Closed"
STATUS_DISMISSED = "Dismissed"
CHAPTER_07 = "07"
CHAPTER_11 = "11"
CHAPTER_12 = "12"
CHAPTER_13 = "13"


@pytest.fixture(scope="module")
def spark():
    # Initialize the SparkSession
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("PySpark Unit Testing") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()


def test_calculate_consumer_information_indicator_valid(spark):
    from your_module import calculate_consumer_information_indicator

    schema = StructType([
        StructField(ACCOUNT_ID_COL, StringType(), True),
        StructField(BANKRUPTCY_CHAPTER_COL, StringType(), True),
        StructField(BANKRUPTCY_STATUS_COL, StringType(), True)
    ])

    input_data = [
        ("1", CHAPTER_07, STATUS_OPEN),
        ("2", CHAPTER_11, STATUS_OPEN),
        ("3", CHAPTER_12, STATUS_DISCHARGED),
        ("4", CHAPTER_13, STATUS_CLOSED),
        ("5", CHAPTER_07, STATUS_DISMISSED),
    ]

    input_df = spark.createDataFrame(input_data, schema)

    result_df = calculate_consumer_information_indicator(input_df)

    expected_data = [
        ("1", "A"),
        ("2", "B"),
        ("3", "G"),
        ("4", "Q"),
        ("5", "Q"),
    ]

    expected_df = spark.createDataFrame(expected_data, [ACCOUNT_ID_COL, CONSUMER_INFORMATION_COL])

    assert result_df.collect() == expected_df.collect()


def test_calculate_consumer_information_indicator_invalid_values(spark):
    from your_module import calculate_consumer_information_indicator

    schema = StructType([
        StructField(ACCOUNT_ID_COL, StringType(), True),
        StructField(BANKRUPTCY_CHAPTER_COL, StringType(), True),
        StructField(BANKRUPTCY_STATUS_COL, StringType(), True)
    ])

    input_data = [
        ("1", "99", STATUS_OPEN),  # Invalid Bankruptcy Chapter
        ("2", CHAPTER_11, "Unknown"),  # Invalid Bankruptcy Status
    ]

    input_df = spark.createDataFrame(input_data, schema)

    result_df = calculate_consumer_information_indicator(input_df)

    expected_data = [
        ("1", ""),  # Blank for invalid chapter
        ("2", ""),  # Blank for invalid status
    ]

    expected_df = spark.createDataFrame(expected_data, [ACCOUNT_ID_COL, CONSUMER_INFORMATION_COL])

    assert result_df.collect() == expected_df.collect()


def test_calculate_consumer_information_indicator_missing_column(spark):
    from your_module import calculate_consumer_information_indicator

    schema = StructType([
        StructField(ACCOUNT_ID_COL, StringType(), True),
        StructField(BANKRUPTCY_STATUS_COL, StringType(), True)  # Missing "Bankruptcy Chapter"
    ])

    input_data = [
        ("1", STATUS_OPEN),
    ]

    input_df = spark.createDataFrame(input_data, schema)

    with pytest.raises(ValueError, match="Error processing data:"):
        calculate_consumer_information_indicator(input_df)
