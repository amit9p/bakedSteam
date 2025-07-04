
# ecbr_calculations/fields/constants.py
DELQ_STATUS_NULL    = None
DELQ_STATUS_001     = "001"
DELQ_STATUS_002     = "002"
DELQ_STATUS_003     = "003"
DELQ_STATUS_004     = "004"
DELQ_STATUS_005     = "005"
DELQ_STATUS_006     = "006"
DELQ_STATUS_DEFAULT = "007"


from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

from ecbr_card_self_service.ecbr_calculations.fields.base_payment_rating import calculate_payment_rating
from ecbr_calculations.fields.constants import (
    DELQ_STATUS_NULL,
    DELQ_STATUS_001,
    DELQ_STATUS_002,
    DELQ_STATUS_003,
    DELQ_STATUS_004,
    DELQ_STATUS_005,
    DELQ_STATUS_006,
    DELQ_STATUS_DEFAULT,
)

def calculate_delinquency_status(
    account_df: DataFrame,
    customer_df: DataFrame,
    recoveries_df: DataFrame,
    generated_fields_df: DataFrame,
    fraud_df: DataFrame,
    caps_df: DataFrame
) -> DataFrame:
    payment_rating_df = calculate_payment_rating(
        account_df, customer_df, recoveries_df,
        generated_fields_df, fraud_df, caps_df
    )

    return payment_rating_df.select(
        col("account_id"),
        when(col("payment_rating").isNull(), DELQ_STATUS_NULL)
        .when(col("payment_rating") == 0, DELQ_STATUS_001)
        .when(col("payment_rating") == 1, DELQ_STATUS_002)
        .when(col("payment_rating") == 2, DELQ_STATUS_003)
        .when(col("payment_rating") == 3, DELQ_STATUS_004)
        .when(col("payment_rating") == 4, DELQ_STATUS_005)
        .when(col("payment_rating") == 5, DELQ_STATUS_006)
        .otherwise(DELQ_STATUS_DEFAULT)
        .alias("delinquency_status")
    )


----



from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

from ecbr_card_self_service.ecbr_calculations.fields.base_payment_rating import calculate_payment_rating

def calculate_delinquency_status(
    account_df: DataFrame,
    customer_df: DataFrame,
    recoveries_df: DataFrame,
    generated_fields_df: DataFrame,
    fraud_df: DataFrame,
    caps_df: DataFrame
) -> DataFrame:
    # 1) get your intermediate DF with payment_rating
    payment_rating_df = calculate_payment_rating(
        account_df, customer_df, recoveries_df,
        generated_fields_df, fraud_df, caps_df
    )

    # 2) map payment_rating → 3-digit delinquency_status
    result_df = payment_rating_df.select(
        col("account_id"),
        when(col("payment_rating").isNull(), None)    .when(col("payment_rating") == 0,  "001")
        .when(col("payment_rating") == 1,  "002")     .when(col("payment_rating") == 2,  "003")
        .when(col("payment_rating") == 3,  "004")     .when(col("payment_rating") == 4,  "005")
        .when(col("payment_rating") == 5,  "006")     .otherwise(                  "007")
        .alias("delinquency_status")
    )

    return result_df
