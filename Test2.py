

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
