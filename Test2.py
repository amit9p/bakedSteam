
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ab.passthrough import get_current_credit_limit, get_original_credit_limit

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").appName("test").getOrCreate()

@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField(ABSegment.account_id.str, StringType(), True),
        StructField(CCAccount.available_spending_amount.str, DoubleType(), True)
    ])

    data = [
        ("A001", 1000.75),
        ("A002", 2499.49),
        ("A003", 0.0),
        ("A004", None)
    ]

    return spark.createDataFrame(data, schema)

def test_get_current_credit_limit(sample_df):
    result_df = get_current_credit_limit(sample_df)

    result = {row[ABSegment.account_id.str]: row[ABSegment.current_credit_limit.str] for row in result_df.collect()}

    assert result["A001"] == 1001
    assert result["A002"] == 2499
    assert result["A003"] == 0
    assert result["A004"] is None

def test_get_original_credit_limit(sample_df):
    result_df = get_original_credit_limit(sample_df)

    result = {row[ABSegment.account_id.str]: row[ABSegment.original_credit_limit.str] for row in result_df.collect()}

    assert result["A001"] == 1001
    assert result["A002"] == 2499
    assert result["A003"] == 0
    assert result["A004"] is None






def get_original_credit_limit(input_df: DataFrame) -> DataFrame:
    """
    Returns account_id and original_credit_limit (same as current_credit_limit),
    rounded to whole dollar as per AB Field 20 definition.
    If current_credit_limit is not already available, it is computed internally.
    """

    # Ensure current_credit_limit is present by calling the method
    current_credit_df = get_current_credit_limit(input_df)

    # Create original_credit_limit from current_credit_limit
    result_df = current_credit_df.withColumn(
        ABSegment.original_credit_limit.str,
        col(ABSegment.current_credit_limit.str)
    )

    return result_df.select(
        ABSegment.account_id,
        ABSegment.original_credit_limit
    )





from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment

def get_original_credit_limit(input_df: DataFrame) -> DataFrame:
    """
    Returns account_id and original_credit_limit (same as current_credit_limit),
    rounded to whole dollar as per AB Field 20 definition.
    """
    result_df = input_df.withColumn(
        ABSegment.original_credit_limit.str,
        col(ABSegment.current_credit_limit.str)  # Already rounded in previous method
    )

    return result_df.select(
        ABSegment.account_id,
        ABSegment.original_credit_limit
    )






from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment

def get_current_credit_limit(ecbr_df: DataFrame) -> DataFrame:
    """
    Returns account_id and current_credit_limit (rounded to whole dollar)
    from the spending_limit field in ecbr_df.
    """
    result_df = ecbr_df.withColumn(
        ABSegment.current_credit_limit.str,
        round(col("spending_limit"))
    )

    return result_df.select(
        ABSegment.account_id,
        ABSegment.current_credit_limit
    )
