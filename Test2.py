
(
    df
    .coalesce(1)                 # force single output file
    .write
    .mode("overwrite")           # or "append"
    .option("header", "true")
    .csv("/path/to/output/dir")
)

from datetime import datetime

from chispa import assert_df_equality
from pyspark.sql import SparkSession
from typedpyspark import create_partially_filled_dataset

# ✅ adjust these imports to match your repo
from ecbr_card_self_service.schemas.consolidated_card_accounts_customers import (
    ConsolidatedCardAccountsCustomers,
)
from ecbr_card_self_service.ecbr_calculations.fields.base.date_of_first_delinquency import (
    date_of_first_delinquency,  # new function: accepts consolidated_df, returns (account_id, date_of_first_delinquency)
)

from ecbr_card_self_service.ecbr_calculations import constants


def test_date_of_first_delinquency_one_df(spark: SparkSession):
    # ----------------------------
    # 1) Build ONE consolidated DF
    # ----------------------------
    consolidated_df = create_partially_filled_dataset(
        spark,
        ConsolidatedCardAccountsCustomers,
        data={
            ConsolidatedCardAccountsCustomers.account_id: [
                "1", "2", "3", "4", "5", "6", "7", "8", "9", "999", "8888",
                "R1", "R2", "R3", "R4", "R5", "R6",
            ],

            # Input "most recent 30dpd date" (earlier it was CCAccount.date_of_first_delinquency)
            ConsolidatedCardAccountsCustomers.date_of_first_delinquency: [
                datetime(2024, 1, 13).date(),  # 1
                datetime(2024, 1, 13).date(),  # 2
                datetime(2024, 1, 13).date(),  # 3
                None,                          # 4
                datetime(2024, 1, 13).date(),  # 5
                datetime(2024, 1, 12).date(),  # 6
                None,                          # 7
                datetime(2024, 1, 12).date(),  # 8
                datetime(2024, 1, 13).date(),  # 9
                None,                          # 999
                datetime(2025, 7, 2).date(),   # 8888

                datetime(2024, 1, 13).date(),  # R1
                datetime(2024, 1, 20).date(),  # R2
                datetime(2024, 1, 20).date(),  # R3
                None,                          # R4
                None,                          # R5
                datetime(2024, 2, 10).date(),  # R6
            ],

            ConsolidatedCardAccountsCustomers.account_open_date: [
                datetime(2024, 1, 11).date(),  # 1
                datetime(2024, 1, 11).date(),  # 2
                datetime(2024, 1, 11).date(),  # 3
                datetime(2000, 1, 1).date(),   # 4
                datetime(2024, 1, 13).date(),  # 5
                None,                          # 6
                datetime(2024, 1, 13).date(),  # 7
                None,                          # 8
                None,                          # 9
                None,                          # 999
                None,                          # 8888

                datetime(2020, 1, 1).date(),   # R1
                datetime(2020, 1, 1).date(),   # R2
                datetime(2020, 1, 1).date(),   # R3
                datetime(2021, 5, 5).date(),   # R4
                None,                          # R5
                datetime(2022, 7, 7).date(),   # R6
            ],

            ConsolidatedCardAccountsCustomers.reactivation_status: [
                constants.ReactivationNotification.REACTIVATED.value.upper(),           # 1
                constants.ReactivationNotification.REACTIVATION_DECLINED.value,         # 2
                constants.ReactivationNotification.REACTIVATION_DECLINED.value.upper(), # 3
                "", "", "", "", "", "", "", "",

                constants.ReactivationNotification.REACTIVATED.value,  # R1
                None,  # R2
                None,  # R3
                None,  # R4
                None,  # R5
                None,  # R6
            ],

            ConsolidatedCardAccountsCustomers.bankruptcy_court_case_status_code: [
                constants.BankruptcyStatus.OPEN.value,                            # 1
                constants.BankruptcyStatus.CLOSED.value,                          # 2
                constants.BankruptcyStatus.CONVERTED_BK_ACCT_NO_DATA_AVAIL.value, # 3
                constants.BankruptcyStatus.DISMISSED.value,                       # 4
                constants.BankruptcyStatus.OPEN.value,                            # 5
                constants.BankruptcyStatus.CLOSED.value,                          # 6
                constants.BankruptcyStatus.CONVERTED_BK_ACCT_NO_DATA_AVAIL.value, # 7
                constants.BankruptcyStatus.DISMISSED.value,                       # 8
                None,                                                            # 9
                constants.BankruptcyStatus.OPEN.value,                            # 999
                constants.BankruptcyStatus.OPEN.value,                            # 8888

                None,                           # R1
                constants.BankruptcyStatus.OPEN.value,  # R2
                constants.BankruptcyStatus.OPEN.value,  # R3
                None,                           # R4
                None,                           # R5
                None,                           # R6
            ],

            ConsolidatedCardAccountsCustomers.bankruptcy_case_file_date: [
                datetime(2023, 12, 31).date(),  # 1
                datetime(2023, 12, 31).date(),  # 2
                datetime(2024, 1, 20).date(),   # 3
                datetime(2024, 1, 13).date(),   # 4
                datetime(2024, 1, 1).date(),    # 5
                None,                           # 6
                None,                           # 7
                None,                           # 8
                datetime(2024, 12, 31).date(),  # 9
                datetime(2024, 1, 1).date(),    # 999
                None,                           # 8888

                None,                           # R1
                datetime(2024, 1, 10).date(),   # R2  (bk < 30dpd => Rule2)
                datetime(2024, 1, 25).date(),   # R3  (bk >= 30dpd => Rule3)
                None,                           # R4
                None,                           # R5
                None,                           # R6
            ],
        },
    )

    # ----------------------------
    # 2) Call new function
    # ----------------------------
    result_df = date_of_first_delinquency(consolidated_df)

    # ----------------------------
    # 3) Expected (ONLY 2 columns)
    # ----------------------------
    expected_df = create_partially_filled_dataset(
        spark,
        ConsolidatedCardAccountsCustomers,  # we can still use schema helper, but will only select 2 cols below
        data={
            ConsolidatedCardAccountsCustomers.account_id: [
                "1", "2", "3", "4", "5", "6", "7", "8", "9", "999", "8888",
                "R1", "R2", "R3", "R4", "R5", "R6",
            ],
            ConsolidatedCardAccountsCustomers.date_of_first_delinquency: [
                None,                                         # 1
                datetime(2023, 12, 31).date(),                 # 2
                datetime(2024, 1, 13).date(),                  # 3
                datetime(2000, 1, 1).date(),                   # 4
                datetime(2024, 1, 1).date(),                   # 5
                datetime(2024, 1, 12).date(),                  # 6
                datetime(2024, 1, 13).date(),                  # 7
                datetime(2024, 1, 12).date(),                  # 8
                datetime(2024, 1, 13).date(),                  # 9
                constants.DEFAULT_ERROR_DATE,                  # 999 => 1900-01-01
                datetime(2025, 7, 2).date(),                   # 8888

                None,                                         # R1
                datetime(2024, 1, 10).date(),                  # R2
                datetime(2024, 1, 20).date(),                  # R3
                datetime(2021, 5, 5).date(),                   # R4
                constants.DEFAULT_ERROR_DATE,                  # R5 => 1900-01-01
                datetime(2024, 2, 10).date(),                  # R6
            ],
        },
    )

    # ----------------------------
    # 4) Compare ONLY the 2 cols
    # ----------------------------
    actual = result_df.select(
        ConsolidatedCardAccountsCustomers.account_id,
        ConsolidatedCardAccountsCustomers.date_of_first_delinquency,
    ).orderBy(ConsolidatedCardAccountsCustomers.account_id)

    expected = expected_df.select(
        ConsolidatedCardAccountsCustomers.account_id,
        ConsolidatedCardAccountsCustomers.date_of_first_delinquency,
    ).orderBy(ConsolidatedCardAccountsCustomers.account_id)

    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)

______________


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
