

import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from ecb_tenant_card_dfs_li.ecbr_generator.reportable_accounts import get_reportable_accounts
from ecb_tenant_card_dfs_li.ecbr_calculations.create_partially_filled_dataset import create_partially_filled_dataset

# Import your typed dataset schema classes
from ecb_tenant_card_dfs_li.schemas.calculator_schema import CalculatorSchema
from ecb_tenant_card_dfs_li.schemas.credit_bureau_account_schema import CreditBureauAccountSchema
from ecb_tenant_card_dfs_li.schemas.reporting_override_schema import ReportingOverrideSchema
from ecb_tenant_card_dfs_li.schemas.customer_dm_os_schema import CustomerDmOsSchema


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("test_reportable_accounts")
        .getOrCreate()
    )


def test_get_reportable_accounts_core(spark):
    """
    Unit test for get_reportable_accounts() using create_partially_filled_dataset() and assert_df_equality().
    """

    # --- 1️⃣ Create input datasets ---
    calculator_df = create_partially_filled_dataset(
        spark,
        CalculatorSchema,
        data=[
            {"account_id": "A1", "account_status": "OP"},
            {"account_id": "A2", "account_status": "DA"},
            {"account_id": "A3", "account_status": "OP"},
            {"account_id": "A4", "account_status": "OP"},
            {"account_id": "A5", "account_status": "OP"},
            {"account_id": "A6", "account_status": "DA"},
        ],
    )

    credit_bureau_account_df = create_partially_filled_dataset(
        spark,
        CreditBureauAccountSchema,
        data=[
            {"account_id": "A3", "reporting_status": "S"},  # Suppressed by C1
        ],
    )

    reporting_override_df = create_partially_filled_dataset(
        spark,
        ReportingOverrideSchema,
        data=[
            {"account_id": "A4", "close_date": None},  # Active override (suppressed)
        ],
    )

    customer_dm_os_df = create_partially_filled_dataset(
        spark,
        CustomerDmOsSchema,
        data=[
            # A5 → all S (suppressed if account_status != 'DA')
            {"account_id": "A5", "customer_id": "C1", "reporting_status": "S"},
            {"account_id": "A5", "customer_id": "C2", "reporting_status": "S"},
            # A6 → all S but account_status = 'DA' (NOT suppressed)
            {"account_id": "A6", "customer_id": "C3", "reporting_status": "S"},
            {"account_id": "A6", "customer_id": "C4", "reporting_status": "S"},
            # A1 → mix of statuses (not all S)
            {"account_id": "A1", "customer_id": "C5", "reporting_status": "S"},
            {"account_id": "A1", "customer_id": "C6", "reporting_status": "A"},
        ],
    )

    # --- 2️⃣ Run the function under test ---
    result_df = get_reportable_accounts(
        calculator_df=calculator_df,
        credit_bureau_account_df=credit_bureau_account_df,
        reporting_override_df=reporting_override_df,
        customer_dm_os_df=customer_dm_os_df,
    )

    # --- 3️⃣ Define expected output ---
    expected_df = create_partially_filled_dataset(
        spark,
        CalculatorSchema,
        data=[
            {"account_id": "A1", "account_status": "OP"},
            {"account_id": "A2", "account_status": "DA"},
            {"account_id": "A6", "account_status": "DA"},
        ],
    )

    # --- 4️⃣ Assert equality ---
    assert_df_equality(
        result_df.select("account_id", "account_status").orderBy("account_id"),
        expected_df.select("account_id", "account_status").orderBy("account_id"),
        ignore_row_order=True,
        ignore_nullable=True,
    )


def test_empty_inputs_yield_empty(spark):
    """
    Ensures function returns empty DataFrame when given empty inputs.
    """
    empty_calculator_df = create_partially_filled_dataset(
        spark, CalculatorSchema, data=[]
    )
    empty_cba_df = create_partially_filled_dataset(
        spark, CreditBureauAccountSchema, data=[]
    )
    empty_override_df = create_partially_filled_dataset(
        spark, ReportingOverrideSchema, data=[]
    )
    empty_customer_df = create_partially_filled_dataset(
        spark, CustomerDmOsSchema, data=[]
    )

    result_df = get_reportable_accounts(
        calculator_df=empty_calculator_df,
        credit_bureau_account_df=empty_cba_df,
        reporting_override_df=empty_override_df,
        customer_dm_os_df=empty_customer_df,
    )

    assert result_df.count() == 0
