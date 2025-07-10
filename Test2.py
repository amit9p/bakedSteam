
from pyspark.sql.functions import col, when, lower

return payment_rating_df.select(
    col(ABSegment.account_id).alias("account_id"),
    when(
        col(ABSegment.payment_rating).isNull(), 
        constants.DELQ_STATUS_NULL
    )
    .when(
        lower(col(ABSegment.payment_rating)) == constants.DELQ_STATUS_001.lower(),
        constants.DELQ_STATUS_001
    )
    .when(
        lower(col(ABSegment.payment_rating)) == constants.DELQ_STATUS_002.lower(),
        constants.DELQ_STATUS_002
    )
    # …and so on for 003 through 007 …
    .otherwise(constants.DEFAULT_ERROR_STRING)
    .alias("delinquency_status")
)
