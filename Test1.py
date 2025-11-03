
from pyspark.sql import functions as F

def get_reportable_accounts(calculator_df, credit_bureau_account_df, reporting_override_df, customer_dm_os_df):
    """
    This method filters only reportable accounts based on suppression logic.

    Suppression logic:
    1. Account is suppressed if:
       - reporting_status == 'S' in credit_bureau_account_df, OR
       - there is any active override (close_date is NULL) in reporting_override_df, OR
       - customer/account ID exists in customer_dm_os_df with reporting_status == 'S'
    2. AND account_status != 'DA' in calculator_df
    """

    # --- Prepare suppression flags ---

    # 1. Suppressed based on reporting_status == 'S'
    suppressed_by_status_df = credit_bureau_account_df.select("account_id", "reporting_status") \
        .where(F.col("reporting_status") == "S") \
        .withColumn("is_suppressed_by_status", F.lit(True))

    # 2. Suppressed based on active overrides (close_date is null)
    suppressed_by_override_df = reporting_override_df.select("account_id", "close_date") \
        .where(F.col("close_date").isNull()) \
        .withColumn("is_suppressed_by_override", F.lit(True))

    # 3. Suppressed if account/customer_id found in customer_dm_os with reporting_status == 'S'
    suppressed_by_customer_df = customer_dm_os_df.select("account_id", "reporting_status") \
        .where(F.col("reporting_status") == "S") \
        .withColumn("is_suppressed_by_customer", F.lit(True))

    # --- Combine all suppression criteria (OR condition) ---
    combined_suppression_df = calculator_df.alias("calc") \
        .join(suppressed_by_status_df.alias("status"), F.col("calc.account_id") == F.col("status.account_id"), "left") \
        .join(suppressed_by_override_df.alias("override"), F.col("calc.account_id") == F.col("override.account_id"), "left") \
        .join(suppressed_by_customer_df.alias("cust"), F.col("calc.account_id") == F.col("cust.account_id"), "left") \
        .withColumn(
            "is_suppressed",
            F.when(
                (F.col("is_suppressed_by_status") == True)
                | (F.col("is_suppressed_by_override") == True)
                | (F.col("is_suppressed_by_customer") == True),
                F.lit(True)
            ).otherwise(F.lit(False))
        )

    # --- Final filter ---
    # Keep only accounts that are NOT suppressed and account_status != 'DA'
    reportable_df = combined_suppression_df.filter(
        (F.col("is_suppressed") == False)
        & (F.col("account_status") != "DA")
    )

    # Return the final set of reportable accounts
    return reportable_df.select([c for c in calculator_df.columns])


________
import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from ecb_tenant_card_dfs_li.ecbr_generator.reportable_accounts import get_reportable_accounts
from ecb_tenant_card_dfs_li.ecbr_calculations.create_partially_filled_dataset import create_partially_filled_dataset


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").appName("test_reportable_accounts").getOrCreate()


def test_get_reportable_accounts(spark):
    """
    Unit test for get_reportable_accounts() using create_partially_filled_dataset() and assert_df_equality().
    """

    # --- 1. Input DataFrames using create_partially_filled_dataset ---

    calculator_df = create_partially_filled_dataset(
        spark,
        [
            {"account_id": "A1", "account_status": "OP"},   # should remain (not suppressed)
            {"account_id": "A2", "account_status": "DA"},   # filtered out (DA)
            {"account_id": "A3", "account_status": "OP"},   # suppressed by status
            {"account_id": "A4", "account_status": "OP"},   # suppressed by override
            {"account_id": "A5", "account_status": "OP"},   # suppressed by customer
        ]
    )

    credit_bureau_account_df = create_partially_filled_dataset(
        spark,
        [
            {"account_id": "A3", "reporting_status": "S"},  # suppressed by status
        ]
    )

    reporting_override_df = create_partially_filled_dataset(
        spark,
        [
            {"account_id": "A4", "close_date": None},  # active override (suppressed)
        ]
    )

    customer_dm_os_df = create_partially_filled_dataset(
        spark,
        [
            {"account_id": "A5", "reporting_status": "S"},  # suppressed by customer
        ]
    )

    # --- 2. Call the actual method ---
    result_df = get_reportable_accounts(
        calculator_df=calculator_df,
        credit_bureau_account_df=credit_bureau_account_df,
        reporting_override_df=reporting_override_df,
        customer_dm_os_df=customer_dm_os_df
    )

    # --- 3. Expected Output ---
    expected_df = create_partially_filled_dataset(
        spark,
        [
            {"account_id": "A1", "account_status": "OP"},  # Only A1 is reportable
        ]
    )

    # --- 4. Assert equality ---
    assert_df_equality(result_df.select("account_id", "account_status"), expected_df.select("account_id", "account_status"))
