def test_without_edq(self, calculated_df, consolidated_df):
    result_df = get_reportable_accounts(
        calculated_dataset=calculated_df,
        consolidated_dataset=consolidated_df,
        context={"product_type": "consumer"},
        edq_suppressions_df=None,
    )

    result_accounts = [r.account_id for r in result_df.select("account_id").collect()]

    assert result_df.count() > 0
    assert "1" in result_accounts
    assert "2" in result_accounts


def test_edq_suppression(self, calculated_df, consolidated_df, edq_suppressions_df):
    result_df = get_reportable_accounts(
        calculated_dataset=calculated_df,
        consolidated_dataset=consolidated_df,
        context={"product_type": "consumer"},
        edq_suppressions_df=edq_suppressions_df,
    )

    result_accounts = [r.account_id for r in result_df.select("account_id").collect()]

    assert "1" in result_accounts
    assert "2" not in result_accounts

<><><><><><><

def get_reportable_accounts(
    calculated_dataset: DataFrame,
    consolidated_dataset: DataFrame,
    context: dict = None,
    edq_suppressions_df: DataFrame = None,
) -> DataFrame:


if edq_suppressions_df is not None:
    edq_accounts_df = (
        edq_suppressions_df
        .select("account_id")
        .dropDuplicates()
    )

    reportable_accounts_df = reportable_accounts_df.join(
        F.broadcast(edq_accounts_df),
        on="account_id",
        how="left_anti"
    )




context.result = get_reportable_accounts(
    context.calculated_df,
    context.consolidated_df,
    context={"product_type": product_type},
    edq_suppressions_df=None,
)

def test_reportable_account_without_edq_suppression(
    clean_calculator_input,
    clean_consolidator_input,
):
    result = get_reportable_accounts(
        clean_calculator_input,
        clean_consolidator_input,
        context={"product_type": "consumer"},
        edq_suppressions_df=None,
    )

    assert result.count() == 1

    account_ids = [row.account_id for row in result.select("account_id").collect()]
    assert "ACC001" in account_ids



import pytest

@pytest.fixture
def edq_suppressions_df(spark):
    data = [
        {
            "account_id": "ACC001",
            "enterprise_servicing_customer_id": "CUST001",
            "run_id": "run_123",
            "run_date": "2026-01-01",
            "ecbr_suppressing_component": "edq_test",
            "run_id_utc_timestamp": "2026-01-01T00:00:00Z",
            "run_id_date": "2026-01-01",
        }
    ]
    return spark.createDataFrame(data)





def test_reportable_account_removed_by_edq_suppression(
    clean_calculator_input,
    clean_consolidator_input,
    edq_suppressions_df,
):
    result = get_reportable_accounts(
        clean_calculator_input,
        clean_consolidator_input,
        context={"product_type": "consumer"},
        edq_suppressions_df=edq_suppressions_df,
    )

    assert result.count() == 0





def test_non_matching_edq_suppression_does_not_remove_reportable_account(
    spark,
    clean_calculator_input,
    clean_consolidator_input,
):
    edq_data = [
        {
            "account_id": "SOME_OTHER_ACCOUNT",
            "enterprise_servicing_customer_id": "CUST999",
            "run_id": "run_456",
            "run_date": "2026-01-01",
            "ecbr_suppressing_component": "edq_test",
            "run_id_utc_timestamp": "2026-01-01T00:00:00Z",
            "run_id_date": "2026-01-01",
        }
    ]
    edq_df = spark.createDataFrame(edq_data)

    result = get_reportable_accounts(
        clean_calculator_input,
        clean_consolidator_input,
        context={"product_type": "consumer"},
        edq_suppressions_df=edq_df,
    )

    assert result.count() == 1

    account_ids = [row.account_id for row in result.select("account_id").collect()]
    assert "ACC001" in account_ids





%%%%%%%%%%%%%%%%%%%










    

python3.11 --version
cd /Users/vmq634/Desktop/EDQ/starship/ecbr-tenant-card
pipenv --rm
pipenv --python 3.11
pipenv install --dev
pipenv run python --version
pipenv run python -c "import pyspark; print(pyspark.__version__)"
pipenv run pytest tests/ecbr_generator/unit_tests/test_reportable_accounts.py -q
pipenv run behave tests/ecbr_generator/features/reportable_accounts.feature


from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("smoke")
    .master("local[1]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

df = spark.createDataFrame([{"account_id": "A1"}])
print(df.count())
spark.stop()


python3.11 --version
cd /Users/vmq634/Desktop/EDQ/starship/ecbr-tenant-card
pipenv --rm
pipenv --python 3.11
pipenv install --dev
pipenv run python --version
pipenv run python -c "import pyspark; print(pyspark.__version__)"
pipenv run pytest tests/ecbr_generator/unit_tests/test_reportable_accounts.py -q
pipenv run behave tests/ecbr_generator/features/reportable_accounts.feature



print("---- smoke test 1: empty df with consolidated schema ----")
empty_df = context.spark.createDataFrame(
    [],
    schema=consolidated_schema.get_structtype()
)
print("empty consolidated df created:", empty_df.count())

print("---- smoke test 2: one tiny df without schema ----")
tiny_df = context.spark.createDataFrame([
    {"account_id": "ACC001"}
])
print("tiny df created:", tiny_df.count())

print("---- smoke test 3: one tiny df with consolidated schema ----")
tiny_consolidated = context.spark.createDataFrame(
    [{
        "account_id": "ACC001",
        "sor_id": "SOR001",
        "sor_customer_id": "CUST001",
        "account_as_of_date_of_data": None,
        "last_reported_compliance_condition_code": None,
        "is_account_paid_in_full": False,
        "is_account_terminal": False,
        "is_account_aged_debt": False,
        "settled_in_full_notification": False,
        "is_account_holder_deceased": False,
        "status_desc": "ACTIVE",
        "is_debt_sold": False,
        "is_credit_bureau_manual_reporting": False,
        "pre_charge_off_settled_in_full_notification": False,
        "account_open_date": None,
        "financial_portfolio_id": None,
        "reactivation_status": None,
        "is_identity_fraud_claimed_on_account": False,
        "block_notification": False,
    }],
    schema=consolidated_schema.get_structtype()
)
print("tiny consolidated df created:", tiny_consolidated.count())

python -c "import pyspark; print(pyspark.__version__)"
