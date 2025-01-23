
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
