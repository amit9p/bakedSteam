# add suppression flags by join, but keep all calculator rows
joined = (
    calculator_df.alias("calc")
    .join(c1, F.col("calc.account_id") == F.col("c1_account_id"), "left")
    .join(c2, F.col("calc.account_id") == F.col("c2_account_id"), "left")
    .join(c3, F.col("calc.account_id") == F.col("c3_account_id"), "left")
    .withColumn(
        "is_suppressed",
        (F.col("c1_account_id").isNotNull())
        | (F.col("c2_account_id").isNotNull())
        | ((F.col("c3_account_id").isNotNull()) & (F.col("account_status") != "DA"))
    )
)

# Now filter from the ORIGINAL calculator_df to preserve duplicates
result = calculator_df.alias("calc") \
    .join(joined.select("calc.account_id", "is_suppressed"), on="account_id", how="left") \
    .filter((F.col("is_suppressed") == False) | F.col("is_suppressed").isNull()) \
    .select(*calculator_df.columns)

_______________

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from ecb_tenant_card_dfs_li.ecbr_generator.reportable_accounts import get_reportable_accounts
from ecb_tenant_card_dfs_li.ecbr_calculations.create_partially_filled_dataset import create_partially_filled_dataset

# typed schemas
from ecb_tenant_card_dfs_li.schemas.calculator_schema import CalculatorSchema
from ecb_tenant_card_dfs_li.schemas.credit_bureau_account_schema import CreditBureauAccountSchema
from ecb_tenant_card_dfs_li.schemas.reporting_override_schema import ReportingOverrideSchema
from ecb_tenant_card_dfs_li.schemas.customer_dm_os_schema import CustomerDmOsSchema


@pytest.fixture(scope="module")
def spark():
    return (SparkSession.builder.master("local[*]")
            .appName("test_reportable_accounts_negative")
            .getOrCreate())


def test_get_reportable_accounts_negative_suppressed_removed(spark):
    """
    Negative test:
      - id=10 -> C3 triggers (ALL customers 'S') AND account_status != 'DA'  -> suppressed
      - id=11 -> C1 triggers (reporting_status='S' in credit_bureau_account)  -> suppressed
      - id=12 -> C2 triggers (active override close_date IS NULL)             -> suppressed
      - id=13 -> ALL customers 'S' but account_status='DA'  -> NOT suppressed (C3 exempt)
      - id=14 -> mix customers (not all 'S'), no C1/C2      -> NOT suppressed
    Expect only ids 13 and 14 in the output.
    """

    calculator_df = create_partially_filled_dataset(
        spark, CalculatorSchema, data=[
            {"account_id": 10, "account_status": "OP"},
            {"account_id": 11, "account_status": "OP"},
            {"account_id": 12, "account_status": "OP"},
            {"account_id": 13, "account_status": "DA"},
            {"account_id": 14, "account_status": "OP"},
        ],
    )

    credit_bureau_account_df = create_partially_filled_dataset(
        spark, CreditBureauAccountSchema, data=[
            {"account_id": 11, "reporting_status": "S"},  # C1
        ],
    )

    reporting_override_df = create_partially_filled_dataset(
        spark, ReportingOverrideSchema, data=[
            {"account_id": 12, "close_date": None},       # C2
        ],
    )

    customer_dm_os_df = create_partially_filled_dataset(
        spark, CustomerDmOsSchema, data=[
            # C3: ALL S for id=10  -> suppressed (since calc != 'DA')
            {"account_id": 10, "customer_id": 1, "reporting_status": "S"},
            {"account_id": 10, "customer_id": 2, "reporting_status": "S"},

            # id=13 -> ALL S but calc='DA' -> NOT suppressed by C3
            {"account_id": 13, "customer_id": 3, "reporting_status": "S"},
            {"account_id": 13, "customer_id": 4, "reporting_status": "S"},

            # id=14 -> NOT all S (so C3 false)
            {"account_id": 14, "customer_id": 5, "reporting_status": "S"},
            {"account_id": 14, "customer_id": 6, "reporting_status": "A"},
        ],
    )

    out_df = get_reportable_accounts(
        calculator_df=calculator_df,
        credit_bureau_account_df=credit_bureau_account_df,
        reporting_override_df=reporting_override_df,
        customer_dm_os_df=customer_dm_os_df,
    )

    # Expected: only ids 13 (DA) and 14 (OP) remain
    expected_df = create_partially_filled_dataset(
        spark, CalculatorSchema, data=[
            {"account_id": 13, "account_status": "DA"},
            {"account_id": 14, "account_status": "OP"},
        ],
    )

    # Strong negative assertions (suppressed ids must be absent)
    assert out_df.filter("account_id IN (10,11,12)").count() == 0

    # Equality on survivors
    assert_df_equality(
        out_df.select("account_id", "account_status").orderBy("account_id"),
        expected_df.select("account_id", "account_status").orderBy("account_id"),
        ignore_row_order=True,
        ignore_nullable=True,
    )
