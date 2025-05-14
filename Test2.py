
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from ecbr_card_self_service.schemas.sbfe.ab_segment import ABSegment

def get_current_credit_limit(ecbr_df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame with account_id and current_credit_limit fields,
    where current_credit_limit is derived from ecbr_df.spending_limit.
    """
    result_df = ecbr_df.withColumn(
        ABSegment.current_credit_limit.str,
        col("spending_limit")
    )

    return result_df.select(
        ABSegment.account_id,
        ABSegment.current_credit_limit
    )
