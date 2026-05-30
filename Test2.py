# diagnose_consolidator_duplicates.py
# Reads the consolidator primary output and checks for duplicate join keys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("diagnose_dupes").getOrCreate()

# ---- EDIT THESE TWO LINES ----
S3_PATH = "s3://ecbr-pfm-s3-it-dfsl1-e1/tenant_data/23b91983-08b7-419c-99bc-a16700d30648-test2/consolidator_outputs/consolidate/enterprise_credit_bureau_reporting_card_dfs_accounts_primary"
RUN_ID  = "23b91983-08b7-419c-99bc-a16700d30648-test2"   # current run_id
# ------------------------------

print(f"Reading from: {S3_PATH}")
df = spark.read.parquet(S3_PATH)

# Scope to current run_id only (very important - otherwise you'd see dupes across runs)
df = df.filter(F.col("run_id") == RUN_ID)

# ===== TEST 1: Overall duplication factor =====
print("\n========== TEST 1: Overall key uniqueness ==========")
total_rows     = df.count()
distinct_keys  = df.select("account_id", "customer_id").distinct().count()
dup_factor     = total_rows / distinct_keys if distinct_keys else 0

print(f"Total rows           : {total_rows:,}")
print(f"Distinct (acct, cust): {distinct_keys:,}")
print(f"Duplication factor   : {dup_factor:.4f}")
print(f"Excess rows (dupes)  : {total_rows - distinct_keys:,}")

if dup_factor > 1.0:
    print(">>> CONFIRMED: consolidator output has duplicates on (account_id, customer_id)")
else:
    print(">>> Keys are unique - duplication is NOT the cause. Look elsewhere (skew, wide rows).")

# ===== TEST 2: Top offenders - which keys have the most dupes =====
print("\n========== TEST 2: Top 20 keys with most duplicates ==========")
dupes_per_key = (
    df.groupBy("account_id", "customer_id")
      .agg(F.count("*").alias("row_count"))
      .filter(F.col("row_count") > 1)
      .orderBy(F.col("row_count").desc())
)
print(f"Number of keys with duplicates: {dupes_per_key.count():,}")
dupes_per_key.show(20, truncate=False)

# ===== TEST 3: Distribution of duplication =====
print("\n========== TEST 3: Distribution of dupe counts ==========")
# How many keys have 2 dupes, 3 dupes, 10 dupes, etc.
dist = (
    dupes_per_key.groupBy("row_count")
                 .agg(F.count("*").alias("num_keys"))
                 .orderBy(F.col("row_count").desc())
)
dist.show(30, truncate=False)

# ===== TEST 4: Inspect a sample duplicate to see what differs =====
print("\n========== TEST 4: Sample duplicate rows ==========")
# Pick the worst offender and show all its rows to see WHY they're duplicated
worst = dupes_per_key.limit(1).collect()
if worst:
    worst_acct = worst[0]["account_id"]
    worst_cust = worst[0]["customer_id"]
    print(f"Inspecting account_id={worst_acct}, customer_id={worst_cust}")
    
    sample = df.filter(
        (F.col("account_id") == worst_acct) &
        (F.col("customer_id") == worst_cust)
    )
    # Show a focused subset of columns - add more as needed
    sample.select(
        "account_id", "customer_id", "open_date", "account_type",
        "credit_limit", "current_balance_amount", "transaction_date",
        "run_id_utc_timestamp"
    ).show(50, truncate=False)
else:
    print("No duplicates found.")

print("\n========== DONE ==========")
spark.stop()
