^\s*$



joined_df = joined_df.filter(F.col("account_type").isNotNull())



"""
reportable_account_count.py

Computes the count of reportable accounts by replicating the consolidator join from
  ecbr_consolidations/datasets/enterprise_credit_bureau_reporting_card_dfs_accounts_primary/consolidate.py

CREDENTIAL-ISOLATION DESIGN (per your instruction):
  Each dataset has its OWN temporary AWS creds tied to its dataset_id. A single
  shared Spark session has only ONE global S3 credential slot, so reading 11
  datasets with 11 different creds collides -> AccessDenied.

  FIX: for EACH dataset we
    1. mint a FRESH OAuth token,
    2. fetch that dataset's AWS creds,
    3. build a DEDICATED Spark session with those creds,
    4. read the partition and WRITE it to local parquet (/tmp staging),
    5. STOP that session.
  Then a FINAL clean session reads all the local parquet (no S3, no creds needed)
  and runs the consolidator join + count.

USAGE:
  - Set INSTNC / LOAD_PART below.
  - Provide MET creds via env vars (preferred) or fill the fallbacks.
  - Run:  python reportable_account_count.py
"""

import os
import json
import shutil
import logging
import subprocess

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# 1. CONFIG
# =============================================================================
INSTNC = "20260608"        # instnc_id-partitioned datasets (YYYYMMDD)
LOAD_PART = "2026-06-08"   # load_partition_date dataset (YYYY-MM-DD)

STAGING_DIR = "/tmp/ecbr_reportable_staging"   # local scratch for materialized datasets

met_clientid = os.environ.get("MET_CLIENTID", "")
met_clientsecret = os.environ.get("MET_CLIENTSECRET", ")

# (name, dataset_id, s3_src_path_with_partition_appended)
DATASETS = [
    (
        "account_service_account",
        "f3af2499-3d5d-4c22-a797-86872a9f1e4d",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/6kes2/lake/recoveries/account_service_account/src/instnc_id=" + INSTNC,
    ),
    (
        "account_service_address",
        "2ff2ef56-4dd3-4636-9f72-659010d9a3ce",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/unoyf/lake/recoveries/account_service_address/src/instnc_id=" + INSTNC,
    ),
    (
        "account_service_customer",
        "90aeffa6-e65d-4d72-b977-55404480f0ca",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/6st45/lake/recoveries/account_service_customer/src/instnc_id=" + INSTNC,
    ),
    (
        "credit_bureau_reporting_service_credit_bureau_customer_dm_os",
        "fe74f988-1f17-4766-a60d-90e9b69ffb52",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/yzuqa/lake/recoveries/credit_bureau_reporting_service_credit_bureau_customer/src/instnc_id=" + INSTNC,
    ),
    (
        "collector_service_account_link",
        "45c7a595-6512-4c8a-83b3-1bc62215dbd4",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/7phje/lake/recoveries/collector_service_account_link/src/instnc_id=" + INSTNC,
    ),
    (
        "collector_service_collector_configuration_dm_os",
        "c1e9ed0f-3e14-41f0-a0a3-ddf30bd5b611",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/4s62x/lake/recoveries/collector_service_collector_configuration/src/instnc_id=" + INSTNC,
    ),
    (
        "credit_bureau_reporting_service_credit_bureau_account_dm_os",
        "7fd7090e-4129-4a45-813c-089ffe0a473c",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/paxjh/lake/recoveries/credit_bureau_reporting_service_credit_bureau_account/src/instnc_id=" + INSTNC,
    ),
    (
        "credit_bureau_reporting_service_reporting_override_dm_os",
        "723741aa-ddd7-4950-ac74-d7b2490d7bf4",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/taxcf/lake/recoveries/credit_bureau_reporting_service_reporting_override/src/instnc_id=" + INSTNC,
    ),
    # ---- transaction datasets: NOT needed for the account COUNT (they only add
    # ---- columns, not account filtering). Kept here but you can drop them.
    (
        "transaction_service_transaction_full_extract_dm_os",
        "87dcc251-8ef8-48bc-884d-b2bf8e28368d",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/cagcy/lake/recoveries/transaction_service_transaction_full_extract/src/instnc_id=" + INSTNC,
    ),
    (
        "transaction_service_journal_dm_os",
        "fadb93ce-e2d0-492f-8ae5-42d6c17c5acc",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/nohth/lake/recoveries/transaction_service_journal/src/instnc_id=" + INSTNC,
    ),
    (
        "transaction_history_service_transaction_history",
        "cbb089f4-b3fc-42a5-a614-6810f8279fdd",
        "s3a://cof-onelake-cat3-qa-useast1/standard/014/gvl4j/lake/src/oneingest-tables/us_card/transaction_history_service_transaction_history/load_partition_date=" + LOAD_PART,
    ),
]

# Only these datasets are actually used in the consolidator join for the COUNT.
# (transaction_* are excluded -- they don't change which accounts are reportable.)
DATASETS_NEEDED_FOR_COUNT = {
    "account_service_account",
    "account_service_address",
    "account_service_customer",
    "credit_bureau_reporting_service_credit_bureau_customer_dm_os",
    "collector_service_account_link",
    "collector_service_collector_configuration_dm_os",
    "credit_bureau_reporting_service_credit_bureau_account_dm_os",
    # reporting_override is read but not used in the join below; keep for completeness.
}

# reportable_accounts_base for the left_anti exclusion. Add when available.
REPORTABLE_ACCOUNTS_BASE = None
# REPORTABLE_ACCOUNTS_BASE = (
#     "enterprise_credit_bureau_reporting_card_metro2_reportable_accounts_base",
#     "<dataset_id>",
#     "s3a://.../enterprise_credit_bureau_reporting_card_metro2_reportable_accounts_base/src/instnc_id=" + INSTNC,
# )

# =============================================================================
# 2. Proxy / Spark env  [copied from dfs_ol_read_qa.py]
# =============================================================================
c1_proxy = "http://proxy-onprem-nlb-us-east-1.cof-prd-bacloudproxy.aws.cb4good.com:8099"
c1_no_proxy = (
    "127.0.0.1,localhost,.local,.internal,169.254.169.254"
    ",.kdc.capitalone.com,.cloud.capitalone.com,.clouddqt.capitalone.com"
    ",.cloud.uk.capitalone.com,.clouddqt.uk.capitalone.com,.cb4good.com,.github.com"
)
os.environ["HTTP_PROXY"] = c1_proxy
os.environ["http_proxy"] = c1_proxy
os.environ["HTTPS_PROXY"] = c1_proxy
os.environ["https_proxy"] = c1_proxy
os.environ["no_proxy"] = c1_no_proxy
os.environ["NO_PROXY"] = c1_no_proxy

java_home = subprocess.check_output(["/usr/libexec/java_home"]).strip().decode("utf-8")
os.environ["JAVA_HOME"] = java_home
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

hadoop_aws_jar = "/Users/vmq634/github/ecbr_assembler/config/hadoop-aws-3.3.4.jar"
aws_java_sdk_jar = "/Users/vmq634/github/ecbr_assembler/config/aws-java-sdk-bundle-1.12.524.jar"

TOKEN_URL = "https://api-it.cloud.capitalone.com/oauth2/token"


# =============================================================================
# 3. Credential + Spark helpers
# =============================================================================
def get_oauth_token() -> str:
    """Fetch a FRESH client-credentials OAuth token. Called per dataset."""
    resp = requests.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials"},
        verify=False,
        allow_redirects=False,
        auth=(met_clientid, met_clientsecret),
    )
    return json.loads(resp.text)["access_token"]


def get_aws_credentials(dataset_id: str, oauth_token: str) -> tuple:
    """Fetch temporary AWS credentials for a given OneLake dataset ID."""
    url = (
        "https://api-it.cloud.capitalone.com/internal-operations/data-management"
        f"/persistence/application-token?datasetId={dataset_id}&consumerOnly=true"
    )
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json;v=2",
        "Authorization": f"Bearer {oauth_token}",
    }
    resp = requests.get(url, headers=headers, verify=False)
    creds = json.loads(resp.text)["credentials"]
    return creds["accessKeyId"], creds["secretAccessKey"], creds["sessionToken"]


def build_spark(app_name, access_key=None, secret_key=None, session_token=None) -> SparkSession:
    """Build a Spark session. If creds are given, wire them for S3 reads."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .config("spark.jars", f"{hadoop_aws_jar},{aws_java_sdk_jar}")
    )
    if access_key is not None:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.session.token", session_token)
        )
    return builder.getOrCreate()


# =============================================================================
# 4. Read ONE dataset in its OWN dedicated session, stage to local parquet, stop.
# =============================================================================
def stage_dataset(name, dataset_id, src_path, staging_dir):
    """Dedicated session per dataset: fresh token -> own creds -> read -> write local -> stop."""
    oauth_token = get_oauth_token()
    ak, sk, st = get_aws_credentials(dataset_id, oauth_token)

    spark = build_spark(f"stage_{name}", ak, sk, st)
    try:
        df = spark.read.parquet(src_path)
        out_path = os.path.join(staging_dir, name)
        # Write to local parquet so the final session can read it WITHOUT any creds.
        df.write.mode("overwrite").parquet("file://" + out_path)
        # Count from the just-written local copy (cheap, and confirms it landed)
        cnt = spark.read.parquet("file://" + out_path).count()
        logger.info(f"Staged {name}: {cnt} rows -> {out_path}")
    finally:
        spark.stop()
    return os.path.join(staging_dir, name)


# =============================================================================
# 5. Main
# =============================================================================
def main():
    # Clean staging dir
    if os.path.exists(STAGING_DIR):
        shutil.rmtree(STAGING_DIR)
    os.makedirs(STAGING_DIR, exist_ok=True)

    # ---- Stage every dataset in its own isolated session ----
    staged = {}
    for name, dataset_id, src_path in DATASETS:
        staged[name] = stage_dataset(name, dataset_id, src_path, STAGING_DIR)

    if REPORTABLE_ACCOUNTS_BASE is not None:
        b_name, b_id, b_path = REPORTABLE_ACCOUNTS_BASE
        staged[b_name] = stage_dataset(b_name, b_id, b_path, STAGING_DIR)

    # ---- Final clean session: read local parquet (NO creds needed) and join ----
    spark = build_spark("reportable_count_final")
    logger.info("Final session built; reading staged local parquet.")

    def local(name):
        return spark.read.parquet("file://" + staged[name])

    account_service_customer_df = local("account_service_customer")
    account_service_account_df = local("account_service_account")
    credit_bureau_customer_df = local("credit_bureau_reporting_service_credit_bureau_customer_dm_os")
    account_service_address_df = local("account_service_address")
    collector_account_link_df = local("collector_service_account_link")
    collector_config_df = local("collector_service_collector_configuration_dm_os")
    credit_bureau_account_df = local("credit_bureau_reporting_service_credit_bureau_account_dm_os")

    # ---- Consolidator join chain (consolidate.py lines 66-97) ----
    joined_df = (
        account_service_customer_df.alias("cust")
        .join(
            account_service_account_df.alias("acct"),
            on=F.col("acct.account_id") == F.col("cust.account_id"),
            how="left",
        )
        .join(
            credit_bureau_customer_df.alias("cbc"),
            on=(F.col("cbc.account_id") == F.col("cust.account_id"))
            & (F.col("cbc.customer_id") == F.col("cust.customer_id")),
            how="left",
        )
        .join(
            account_service_address_df.alias("addr"),
            on=F.col("cust.customer_id") == F.col("addr.customer_id"),
            how="left",
        )
        .join(
            collector_account_link_df.alias("link"),
            on=F.col("acct.account_id") == F.col("link.account_id"),
            how="left",
        )
        .join(
            collector_config_df.alias("cfg"),
            on=F.col("link.collector_configuration_id")
            == F.col("cfg.collector_configuration_id"),
            how="left",
        )
        .join(
            credit_bureau_account_df.alias("cba"),
            on=F.col("cbc.account_id") == F.col("cba.account_id"),
            how="left",
        )
    )
    logger.info("Completed consolidator join chain.")

    # ---- left_anti against reportable_accounts_base (consolidate.py lines 101-108) ----
    if REPORTABLE_ACCOUNTS_BASE is not None:
        b_name = REPORTABLE_ACCOUNTS_BASE[0]
        base_df = local(b_name)
        joined_df = joined_df.join(
            base_df.alias("base"),
            on=F.col("acct.account_id") == F.col("base.account_id"),
            how="left_anti",
        )
        logger.info("Applied left_anti join with metro2 reportable_accounts_base.")
    else:
        logger.warning(
            "REPORTABLE_ACCOUNTS_BASE not set -- skipping left_anti exclusion. "
            "Count is the PRE-exclusion consolidated population, NOT final reportable."
        )

    # ---- PRIMARY_FILTERS (consolidate.py line 112) ----
    joined_df = joined_df.filter(
        F.col("cust.cust_role_cd").isin("PRIMARY", "SECONDARY")
    )

    # ---- Final reportable count (consolidate.py line 162) ----
    final_count = joined_df.select(F.col("cust.account_id")).distinct().count()
    logger.info("=" * 60)
    logger.info(f"instnc_id date          : {INSTNC}")
    logger.info(f"Reportable account count: {final_count}")
    logger.info("=" * 60)
    print(f"\nReportable account count (instnc_id={INSTNC}): {final_count}\n")

    spark.stop()
    return final_count


if __name__ == "__main__":
    main()
