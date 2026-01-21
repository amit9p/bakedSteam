from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, to_date, lower, lit

from ecbr_card_self_service.schemas.base_segment import BaseSegment
from ecbr_card_self_service.schemas.consolidated_card_accounts_customers import (
    ConsolidatedCardAccountsCustomers,
)
import ecbr_card_self_service.ecbr_calculations.constants as constants


def date_of_first_delinquency(consolidated_df: DataFrame) -> DataFrame:
    """
    Intent logic (in order):
      1) If Reactivation Notification = Reactivated -> NULL
      2) If Bankruptcy Status != blank AND Bankruptcy File Date exists AND Bankruptcy File Date < Most Recent 30dpd Date
         -> Bankruptcy File Date
      3) If Bankruptcy Status != blank AND Bankruptcy File Date exists AND Bankruptcy File Date >= Most Recent 30dpd Date
         -> Most Recent 30dpd Date
      4) If Most Recent 30dpd Date is NULL AND Account Open Date != NULL -> Account Open Date
      5) If Most Recent 30dpd Date is NULL AND Account Open Date is NULL -> DEFAULT_ERROR_DATE
      6) Else -> Most Recent 30dpd Date
    """

    # Most recent date customer became 30 days past due
    # (mapped to CCAccount.date_of_first_delinquency previously)
    most_recent_30dpd_date = to_date(
        col(ConsolidatedCardAccountsCustomers.date_of_first_delinquency),
        format=constants.DATE_FORMAT,
    )

    bankruptcy_file_date = to_date(
        col(ConsolidatedCardAccountsCustomers.bankruptcy_case_file_date),
        format=constants.DATE_FORMAT,
    )

    account_open_date = to_date(
        col(ConsolidatedCardAccountsCustomers.account_open_date),
        format=constants.DATE_FORMAT,
    )

    # --- Presence / blank checks ---
    bankruptcy_status_is_present = (
        col(ConsolidatedCardAccountsCustomers.bankruptcy_court_case_status_code).isNotNull()
        & (trim(col(ConsolidatedCardAccountsCustomers.bankruptcy_court_case_status_code)) != "")
    )

    bankruptcy_file_date_is_present = (
        col(ConsolidatedCardAccountsCustomers.bankruptcy_case_file_date).isNotNull()
        & (trim(col(ConsolidatedCardAccountsCustomers.bankruptcy_case_file_date)) != "")
    )

    most_recent_30dpd_is_null_or_blank = (
        col(ConsolidatedCardAccountsCustomers.date_of_first_delinquency).isNull()
        | (trim(col(ConsolidatedCardAccountsCustomers.date_of_first_delinquency)) == "")
    )

    account_open_is_present = (
        col(ConsolidatedCardAccountsCustomers.account_open_date).isNotNull()
        & (trim(col(ConsolidatedCardAccountsCustomers.account_open_date)) != "")
    )

    account_open_is_null_or_blank = (
        col(ConsolidatedCardAccountsCustomers.account_open_date).isNull()
        | (trim(col(ConsolidatedCardAccountsCustomers.account_open_date)) == "")
    )

    # --- Reactivation Rule ---
    reactivation_is_reactivated = (
        lower(trim(col(ConsolidatedCardAccountsCustomers.reactivation_status)))
        == constants.ReactivationNotification.REACTIVATED.value
    )

    # --- Date comparisons (only meaningful when both exist) ---
    bankruptcy_file_is_earlier_than_30dpd = bankruptcy_file_date < most_recent_30dpd_date
    bankruptcy_file_is_later_or_same_as_30dpd = bankruptcy_file_date >= most_recent_30dpd_date

    output_expr = (
        # Rule 1
        when(reactivation_is_reactivated, lit(None))
        # Rule 2
        .when(
            bankruptcy_status_is_present
            & bankruptcy_file_date_is_present
            & (~most_recent_30dpd_is_null_or_blank)
            & bankruptcy_file_is_earlier_than_30dpd,
            bankruptcy_file_date,
        )
        # Rule 3
        .when(
            bankruptcy_status_is_present
            & bankruptcy_file_date_is_present
            & (~most_recent_30dpd_is_null_or_blank)
            & bankruptcy_file_is_later_or_same_as_30dpd,
            most_recent_30dpd_date,
        )
        # Rule 4
        .when(
            most_recent_30dpd_is_null_or_blank & account_open_is_present,
            account_open_date,
        )
        # Rule 5
        .when(
            most_recent_30dpd_is_null_or_blank & account_open_is_null_or_blank,
            lit(constants.DEFAULT_ERROR_DATE),
        )
        # Rule 6
        .otherwise(most_recent_30dpd_date)
    )

    result_df = (
        consolidated_df.withColumn(BaseSegment.date_of_first_delinquency.str, output_expr)
        .select(BaseSegment.account_id, BaseSegment.date_of_first_delinquency)
    )

    return result_df
