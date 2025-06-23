
# Add to CCAccount
CCAccount.account_id = "10"
CCAccount.date_of_first_delinquency = datetime.strptime("2024-01-13", default_date_format)
CCAccount.account_open_date = datetime.strptime("2024-01-13", default_date_format)
CCAccount.is_account_reactivated = ""  # or null
# Make bankruptcy date null in CustomerInformation for this ID

# Add to CustomerInformation
CustomerInformation.account_id = "10"
CustomerInformation.bankruptcy_first_filed_date = None

# Add to expected_data
BaseSegment.account_id = "10"
BaseSegment.date_of_first_delinquency = datetime.strptime("1900-01-01", default_date_format)




from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, to_date, lower, lit

from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.cc_account import CCAccount
from ecbr_card_self_service.schemas.customer_information import CustomerInformation
from ecbr_card_self_service.ecbr_calculations.constants import DEFAULT_ERROR_DATE

expected_default_date_format = "yyyy-MM-dd"

def date_of_first_delinquency(
    account_df: DataFrame, customer_df: DataFrame
) -> DataFrame:

    joined_df = account_df.join(customer_df, on=BaseSegment.account_id.str, how="left")

    bankruptcy_file_date_is_earlier = to_date(
        CustomerInformation.bankruptcy_first_filed_date, format=expected_default_date_format
    ) < to_date(CCAccount.date_of_first_delinquency, format=expected_default_date_format)

    bankruptcy_file_date_is_later = to_date(
        CustomerInformation.bankruptcy_first_filed_date, format=expected_default_date_format
    ) > to_date(CCAccount.date_of_first_delinquency, format=expected_default_date_format)

    bankruptcy_file_date_is_not_null_or_blank = (
        CustomerInformation.bankruptcy_first_filed_date.isNotNull()
        & (trim(CustomerInformation.bankruptcy_first_filed_date) != "")
    )

    start_date_of_delinquency_is_not_null_or_blank = (
        CCAccount.date_of_first_delinquency.isNotNull()
        & (trim(CCAccount.date_of_first_delinquency) != "")
    )

    account_open_date_is_not_null_or_blank = (
        CCAccount.account_open_date.isNotNull()
        & (trim(CCAccount.account_open_date) != "")
    )

    reactivation_notification_yes = (
        lower(col(CCAccount.is_account_reactivated.str)) == "y"
    )

    reactivation_notification_n_or_none = (
        col(CCAccount.is_account_reactivated.str).isNull()
        | (trim(col(CCAccount.is_account_reactivated.str)) == "")
        | (trim(col(CCAccount.is_account_reactivated.str)) == "n")
    )

    result = (
        when(reactivation_notification_yes, None)
        .when(
            reactivation_notification_n_or_none
            & bankruptcy_file_date_is_not_null_or_blank
            & start_date_of_delinquency_is_not_null_or_blank
            & bankruptcy_file_date_is_earlier,
            CustomerInformation.bankruptcy_first_filed_date,
        )
        .when(
            reactivation_notification_n_or_none
            & bankruptcy_file_date_is_not_null_or_blank
            & start_date_of_delinquency_is_not_null_or_blank
            & bankruptcy_file_date_is_later,
            CCAccount.date_of_first_delinquency,
        )
        .when(
            reactivation_notification_n_or_none
            & bankruptcy_file_date_is_not_null_or_blank
            & start_date_of_delinquency_is_not_null_or_blank
            & account_open_date_is_not_null_or_blank,
            CCAccount.account_open_date,
        )
        .when(
            ~bankruptcy_file_date_is_not_null_or_blank
            & ~start_date_of_delinquency_is_not_null_or_blank
            & ~account_open_date_is_not_null_or_blank,
            lit(DEFAULT_ERROR_DATE),
        )
        .otherwise(
            when(
                CCAccount.date_of_first_delinquency.isNull()
                | (trim(CCAccount.date_of_first_delinquency) == ""),
                lit(DEFAULT_ERROR_DATE)
            ).otherwise(CCAccount.date_of_first_delinquency)
        )
    )

    delinquency_df = joined_df.withColumn(
        BaseSegment.date_of_first_delinquency.str,
        result.cast("date")
    ).select(
        BaseSegment.account_id, BaseSegment.date_of_first_delinquency
    )

    return delinquency_df
