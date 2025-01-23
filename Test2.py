
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

# Define constants for column names and values
BANKRUPTCY_STATUS_COL = "Bankruptcy Status"
BANKRUPTCY_CHAPTER_COL = "Bankruptcy Chapter"

STATUS_OPEN = "Open"
STATUS_DISCHARGED = "Discharge"
STATUS_CLOSED = "Closed"
STATUS_DISMISSED = "Dismissed"

CHAPTER_07 = "07"
CHAPTER_11 = "11"
CHAPTER_12 = "12"
CHAPTER_13 = "13"

def calculate_consumer_information_indicator(input_df: DataFrame) -> DataFrame:
    """
    Calculate Consumer Information Indicator based on Bankruptcy Chapter and Status.

    :param input_df: Input DataFrame containing columns 'Bankruptcy Chapter' and 'Bankruptcy Status'
    :return: DataFrame with 'Account ID' and 'Consumer Information Indicator'
    """
    try:
        result_df = input_df.withColumn(
            "Consumer Information Indicator",
            when(
                (col(BANKRUPTCY_STATUS_COL) == STATUS_OPEN) & (col(BANKRUPTCY_CHAPTER_COL) == CHAPTER_07), "A"
            ).when(
                (col(BANKRUPTCY_STATUS_COL) == STATUS_OPEN) & (col(BANKRUPTCY_CHAPTER_COL) == CHAPTER_11), "B"
            ).when(
                (col(BANKRUPTCY_STATUS_COL) == STATUS_OPEN) & (col(BANKRUPTCY_CHAPTER_COL) == CHAPTER_12), "C"
            ).when(
                (col(BANKRUPTCY_STATUS_COL) == STATUS_OPEN) & (col(BANKRUPTCY_CHAPTER_COL) == CHAPTER_13), "D"
            ).when(
                (col(BANKRUPTCY_STATUS_COL) == STATUS_DISCHARGED) & (col(BANKRUPTCY_CHAPTER_COL) == CHAPTER_07), "E"
            ).when(
                (col(BANKRUPTCY_STATUS_COL) == STATUS_DISCHARGED) & (col(BANKRUPTCY_CHAPTER_COL) == CHAPTER_11), "F"
            ).when(
                (col(BANKRUPTCY_STATUS_COL) == STATUS_DISCHARGED) & (col(BANKRUPTCY_CHAPTER_COL) == CHAPTER_12), "G"
            ).when(
                (col(BANKRUPTCY_STATUS_COL) == STATUS_DISCHARGED) & (col(BANKRUPTCY_CHAPTER_COL) == CHAPTER_13), "H"
            ).when(
                col(BANKRUPTCY_STATUS_COL).isin(STATUS_CLOSED, STATUS_DISMISSED), "Q"
            ).otherwise(lit(""))  # Set blank for unmatched cases
        )
        return result_df.select("Account ID", "Consumer Information Indicator")
    except Exception as e:
        raise ValueError(f"Error processing data: {e}")
