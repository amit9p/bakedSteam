
-- Use NOT EXISTS to avoid NULL pitfalls, and check row vs distinct counts
SELECT
  (SELECT COUNT(*) FROM IDENTIFIER($tbl_calculator) WHERE run_id = $run_id) AS calc_rows,
  (SELECT COUNT(DISTINCT CONSUMER_ACCOUNT_NUMBER) FROM IDENTIFIER($tbl_calculator) WHERE run_id = $run_id) AS calc_distinct_accts,
  (SELECT COUNT(*) FROM IDENTIFIER($tbl_reportable) WHERE run_id = $run_id) AS rep_rows,
  (SELECT COUNT(DISTINCT CONSUMER_ACCOUNT_NUMBER) FROM IDENTIFIER($tbl_reportable) WHERE run_id = $run_id) AS rep_distinct_accts;



-- ============================================================
-- CONFIG
-- ============================================================
SET run_id = 'c897ba20-33bd-4d7b-994c-f23e4570272f-DR-04-26-dfsl1-test5';

SET tbl_calculator = 'US_CARD.US_CARD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_CALCULATED_ACCOUNTS_CUSTOMERS_QA_V15_QHDP_CARD_NPI_VW';
SET tbl_reportable = 'US_CARD.US_CARD.ENTERPRISE_CREDIT_BUREAU_REPORTING_CARD_METRO2_REPORTABLE_ACCOUNTS_QA_V10_QHDP_CARD_NPI_VW';

-- ============================================================
-- The 63: in calculator, NOT in generator output
-- (account key = CONSUMER_ACCOUNT_NUMBER)
-- ============================================================
SELECT c.*
FROM   IDENTIFIER($tbl_calculator) c
WHERE  c.run_id = $run_id
  AND  c.CONSUMER_ACCOUNT_NUMBER NOT IN (
        SELECT CONSUMER_ACCOUNT_NUMBER
        FROM   IDENTIFIER($tbl_reportable)
        WHERE  run_id = $run_id)
ORDER BY c.CONSUMER_ACCOUNT_NUMBER;


-- ============================================================
-- CONFIG
-- ============================================================
SET run_id = 20260426;

SET tbl_primary    = 'MY_DB.MY_SCHEMA.PRIMARY';     -- consolidated primary
SET tbl_secondary  = 'MY_DB.MY_SCHEMA.SECONDARY';   -- consolidated secondary
SET tbl_calculator = 'MY_DB.MY_SCHEMA.CALCULATOR';
SET tbl_reportable = 'MY_DB.MY_SCHEMA.REPORTABLE_ACCOUNTS';

-- ============================================================
-- The 63: in calculator, not in reportable
-- ============================================================
SELECT c.*
FROM   IDENTIFIER($tbl_calculator) c
WHERE  c.instnc_id = $run_id
  AND  c.account_id NOT IN (
        SELECT account_id FROM IDENTIFIER($tbl_reportable)
        WHERE instnc_id = $run_id)
ORDER BY c.account_id;




Hi team — we ran ECBR in QA with the new date range and Metro2 reportable accounts uncommented in config. The EDQ step failed — looks like a SecretsManager `explicit deny` on `secretsmanager:GetSecretValue` for the platform credentials secret. Same role/secret as a job that succeeded, so we think it's platform-side. Details in thread 🧵


spark = (SparkSession.builder
    .appName("input_fanout")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate())
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("my_app").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")   # ← add this line

# ... rest of your code


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("input_fanout").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # suppress those WARN spam messages

RUN_ID = "23b91983-08b7-419c-99bc-a16700d30648-test2"
BASE = f"s3://ecbr-pfm-s3-it-dfsl1-e1/tenant_data/{RUN_ID}/preprocess_outputs"

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

# Try these column names in order (case-insensitive)
POSSIBLE_KEY_NAMES = ["account_id", "ACCOUNT_ID", "acct_id", "ACCT_ID", "account_number"]

print(f"{'Dataset':<42} {'Total':>14} {'Distinct':>14} {'Factor':>8}  Key column")
print("=" * 95)

for name, path in INPUTS.items():
    try:
        df = spark.read.parquet(path)
        
        # Find the right column name (case-insensitive)
        df_cols_lower = [c.lower() for c in df.columns]
        key_col = None
        for candidate in POSSIBLE_KEY_NAMES:
            if candidate.lower() in df_cols_lower:
                # use the actual column name (preserving case)
                key_col = df.columns[df_cols_lower.index(candidate.lower())]
                break
        
        if key_col is None:
            print(f"{name:<42}  NO account_id column found.")
            print(f"   Available columns: {df.columns}")
            continue
        
        total = df.count()
        distinct = df.select(key_col).distinct().count()
        factor = total / distinct if distinct else 0
        flag = "  ← FAN-OUT" if factor > 1.01 else ""
        print(f"{name:<42} {total:>14,} {distinct:>14,} {factor:>8.2f}  {key_col}{flag}")
        
    except Exception as e:
        print(f"{name:<42}  ERROR: {str(e)[:60]}")
        continue

print("\nDone.")
