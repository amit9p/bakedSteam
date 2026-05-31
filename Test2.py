
# diagnostic_1_input_fanout.py
# Measures rows-per-account in each consolidator input dataset
# This proves WHICH input(s) are fanning out

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("input_fanout").getOrCreate()

# ---- EDIT: Paths to each consolidator input for the current run ----
RUN_ID = "23b91983-08b7-419c-99bc-a16700d30648-test2"
BASE = f"s3://ecbr-pfm-s3-it-dfsl1-e1/tenant_data/{RUN_ID}/preprocess_outputs"
# Path structure may differ - check the job logs for exact pre_process output paths

INPUTS = {
    "account_service_account":              f"{BASE}/account_service_account",
    "account_service_address":              f"{BASE}/account_service_address",
    "account_service_customer":             f"{BASE}/account_service_customer",
    "collector_service_account_link":       f"{BASE}/collector_service_account_link",
    "collector_service_collector_config":   f"{BASE}/collector_service_collector_configuration",
    "credit_bureau_svc_account":            f"{BASE}/credit_bureau_reporting_service_credit_bureau_account",
    "credit_bureau_svc_customer":           f"{BASE}/credit_bureau_reporting_service_credit_bureau_customer",
    "consolidated_transactions":            f"{BASE}/consolidated_transactions",
    "metro2_reportable_accounts_base":      f"{BASE}/enterprise_credit_bureau_reporting_card_metro2_reportable_accounts_base",
}

print(f"{'Dataset':<42} {'Total':>12} {'Distinct acct':>15} {'Factor':>8}")
print("=" * 82)

results = {}
for name, path in INPUTS.items():
    try:
        df = spark.read.parquet(path)
        total = df.count()
        distinct = df.select("account_id").distinct().count()
        factor = total / distinct if distinct else 0
        results[name] = factor
        flag = "  ← FAN-OUT" if factor > 1.01 else ""
        print(f"{name:<42} {total:>12,} {distinct:>15,} {factor:>8.2f}{flag}")
    except Exception as e:
        print(f"{name:<42} ERROR: {str(e)[:50]}")

print("\n" + "=" * 82)
print("Multiplied factors of fan-out inputs:", end=" ")
combined = 1.0
for name, factor in results.items():
    if factor > 1.01:
        combined *= factor
print(f"{combined:.2f}  (should equal observed consolidator output factor of 4.00)")
