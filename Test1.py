

from pyspark.sql import functions as F

def get_reportable_accounts(
    calculator_df,
    credit_bureau_account_df,
    reporting_override_df,
    customer_dm_os_df,
):
    # --- C1: reporting_status == 'S' in credit_bureau_account (per-account) ---
    c1 = (credit_bureau_account_df
          .filter(F.col("reporting_status") == "S")
          .select(F.col("account_id").alias("c1_account_id"))
          .dropDuplicates())

    # --- C2: any active override (close_date IS NULL) ---
    c2 = (reporting_override_df
          .filter(F.col("close_date").isNull())
          .select(F.col("account_id").alias("c2_account_id"))
          .dropDuplicates())

    # --- C3: ALL customers for an account have reporting_status == 'S' ---
    # all_customers_s = max( nonS_flag ) == 0  -> all S
    c3_all_s = (customer_dm_os_df
                .groupBy("account_id")
                .agg(F.max(F.when(F.col("reporting_status") != "S", 1).otherwise(0)).alias("has_non_s"))
                .filter(F.col("has_non_s") == 0)
                .select(F.col("account_id").alias("c3_account_id")))

    # Join flags to calculator
    joined = (calculator_df.alias("calc")
              .join(c1, F.col("calc.account_id") == F.col("c1_account_id"), "left")
              .join(c2, F.col("calc.account_id") == F.col("c2_account_id"), "left")
              .join(c3_all_s, F.col("calc.account_id") == F.col("c3_account_id"), "left"))

    # Suppressed if C1 OR C2 OR (C3 AND account_status != 'DA')
    result = (joined
              .withColumn(
                  "is_suppressed",
                  (F.col("c1_account_id").isNotNull())
                  | (F.col("c2_account_id").isNotNull())
                  | ((F.col("c3_account_id").isNotNull()) & (F.col("account_status") != "DA"))
              )
              .filter(~F.col("is_suppressed"))
              .select(*calculator_df.columns))

    return result


-----
import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from ecb_tenant_card_dfs_li.ecbr_generator.reportable_accounts import get_reportable_accounts
from ecb_tenant_card_dfs_li.ecbr_calculations.create_partially_filled_dataset import create_partially_filled_dataset

@pytest.fixture(scope="module")
def spark():
    return (SparkSession.builder
            .master("local[*]")
            .appName("test_reportable_accounts")
            .getOrCreate())

def test_get_reportable_accounts_core(spark):
    # calc: A1 ok, A2 DA (only matters for C3), A3 hit C1, A4 hit C2,
    # A5 hit C3 (all S + not DA) -> suppressed,
    # A6 all S + DA -> NOT suppressed by C3.
    calculator_df = create_partially_filled_dataset(spark, [
        {"account_id": "A1", "account_status": "OP"},
        {"account_id": "A2", "account_status": "DA"},
        {"account_id": "A3", "account_status": "OP"},
        {"account_id": "A4", "account_status": "OP"},
        {"account_id": "A5", "account_status": "OP"},
        {"account_id": "A6", "account_status": "DA"},
    ])

    credit_bureau_account_df = create_partially_filled_dataset(spark, [
        {"account_id": "A3", "reporting_status": "S"},  # C1
    ])

    reporting_override_df = create_partially_filled_dataset(spark, [
        {"account_id": "A4", "close_date": None},       # C2
    ])

    # C3 grouping:
    # A5 -> all customers S (suppressed if calc != 'DA') -> suppressed (calc OP)
    # A6 -> all customers S but calc == 'DA' -> NOT suppressed by C3
    # A1 -> mix S/other -> not all S
    customer_dm_os_df = create_partially_filled_dataset(spark, [
        {"account_id": "A5", "customer_id": "C1", "reporting_status": "S"},
        {"account_id": "A5", "customer_id": "C2", "reporting_status": "S"},
        {"account_id": "A6", "customer_id": "C3", "reporting_status": "S"},
        {"account_id": "A6", "customer_id": "C4", "reporting_status": "S"},
        {"account_id": "A1", "customer_id": "C5", "reporting_status": "S"},
        {"account_id": "A1", "customer_id": "C6", "reporting_status": "A"},  # breaks "all S"
    ])

    out_df = get_reportable_accounts(
        calculator_df,
        credit_bureau_account_df,
        reporting_override_df,
        customer_dm_os_df
    )

    # Expected reportable: A1 (not C1/C2; C3 false), A2 (DA doesn't matter for C1/C2; C3 false),
    # A6 (all S but DA -> C3 not applied)
    expected_df = create_partially_filled_dataset(spark, [
        {"account_id": "A1", "account_status": "OP"},
        {"account_id": "A2", "account_status": "DA"},
        {"account_id": "A6", "account_status": "DA"},
    ])

    assert_df_equality(
        out_df.select("account_id", "account_status").orderBy("account_id"),
        expected_df.select("account_id", "account_status").orderBy("account_id"),
        ignore_row_order=True,
        ignore_nullable=True
    )

def test_empty_inputs_yield_empty(spark):
    empty = create_partially_filled_dataset(spark, [])
    out = get_reportable_accounts(empty, empty, empty, empty)
    assert out.count() == 0
