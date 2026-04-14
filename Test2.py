
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
