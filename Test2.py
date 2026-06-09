
"""
reportable_account_count.py

Loads each DFS L1 dataset at a configurable partition (instance) date, replicates
the consolidator join logic from
  ecbr_consolidations/datasets/enterprise_credit_bureau_reporting_card_dfs_accounts_primary/consolidate.py
and reports the count of accounts that would be reported.

Infrastructure (OAuth token, AWS creds, proxy, Spark builder, per-dataset S3 read)
is taken directly from dfs_ol_read_qa.py / dfsl1_ol_dir_list.py.

PARTITION SCHEMES (two different ones across datasets):
  - Most datasets are partitioned by `instnc_id`         with value like 20260608 (YYYYMMDD, no dashes)
  - transaction_history_service_transaction_history is
    partitioned by `load_partition_date`                with value like 2026-06-08 (YYYY-MM-DD, dashes)
Each dataset row in DATASETS carries its own (partition_key, partition_value).

USAGE:
  - Set INSTNC and LOAD_PART below to the date you want.
  - Provide MET creds via env vars (preferred) or fill the fallbacks.
  - Run:  python reportable_account_count.py
"""

import os
import json
import logging
import subprocess

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# 1. DATE CONFIG  -- the knobs you asked for
# =============================================================================
INSTNC = "20260608"        # value for instnc_id-partitioned datasets (YYYYMMDD)
LOAD_PART = "2026-06-08"   # value for the load_partition_date dataset (YYYY-MM-DD)

# Credentials for the MET OAuth call.
# Prefer env vars; the fallbacks here are the active (uncommented) creds from dfs_ol_read_qa.py.
met_clientid = os.environ.get("MET_CLIENTID", "d1586f04f8c0722258dee37fb22a6fa6")
met_clientsecret = os.environ.get("MET_CLIENTSECRET", "dddfa70fc55d91b57ce842411b67db9c")

# =============================================================================
# 2. Dataset config: (name, dataset_id, s3_src_path, partition_key, partition_value)
#    Paths/ids copied from dfsl1_ol_dir_list.py. Note the ODD ONE OUT below.
# =============================================================================
DATASETS = [
    (
        "account_service_account",
        "f3af2499-3d5d-4c22-a797-86872a9f1e4d",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/6kes2/lake/recoveries/account_service_account/src",
        "instnc_id", INSTNC,
    ),
    (
        "transaction_service_transaction_full_extract_dm_os",
        "87dcc251-8ef8-48bc-884d-b2bf8e28368d",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/cagcy/lake/recoveries/transaction_service_transaction_full_extract/src",
        "instnc_id", INSTNC,
    ),
    (
        "transaction_service_journal_dm_os",
        "fadb93ce-e2d0-492f-8ae5-42d6c17c5acc",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/nohth/lake/recoveries/transaction_service_journal/src",
        "instnc_id", INSTNC,
    ),
    (
        # ---- ODD ONE OUT: different bucket + different partition key/format ----
        "transaction_history_service_transaction_history",
        "cbb089f4-b3fc-42a5-a614-6810f8279fdd",
        "s3a://cof-onelake-cat3-qa-useast1/standard/014/gvl4j/lake/src/oneingest-tables/us_card/transaction_history_service_transaction_history",
        "load_partition_date", LOAD_PART,
    ),
    (
        "account_service_address",
        "2ff2ef56-4dd3-4636-9f72-659010d9a3ce",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/unoyf/lake/recoveries/account_service_address/src",
        "instnc_id", INSTNC,
    ),
    (
        "account_service_customer",
        "90aeffa6-e65d-4d72-b977-55404480f0ca",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/6st45/lake/recoveries/account_service_customer/src",
        "instnc_id", INSTNC,
    ),
    (
        "credit_bureau_reporting_service_credit_bureau_customer_dm_os",
        "fe74f988-1f17-4766-a60d-90e9b69ffb52",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/yzuqa/lake/recoveries/credit_bureau_reporting_service_credit_bureau_customer/src",
        "instnc_id", INSTNC,
    ),
    (
        "collector_service_account_link",
        "45c7a595-6512-4c8a-83b3-1bc62215dbd4",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/7phje/lake/recoveries/collector_service_account_link/src",
        "instnc_id", INSTNC,
    ),
    (
        "collector_service_collector_configuration_dm_os",
        "c1e9ed0f-3e14-41f0-a0a3-ddf30bd5b611",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/4s62x/lake/recoveries/collector_service_collector_configuration/src",
        "instnc_id", INSTNC,
    ),
    (
        "credit_bureau_reporting_service_credit_bureau_account_dm_os",
        "7fd7090e-4129-4a45-813c-089ffe0a473c",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/paxjh/lake/recoveries/credit_bureau_reporting_service_credit_bureau_account/src",
        "instnc_id", INSTNC,
    ),
    (
        "credit_bureau_reporting_service_reporting_override_dm_os",
        "723741aa-ddd7-4950-ac74-d7b2490d7bf4",
        "s3a://cof-uscard-lossmitigation-cat3-qa-useast1/taxcf/lake/recoveries/credit_bureau_reporting_service_reporting_override/src",
        "instnc_id", INSTNC,
    ),
]

# The reportable_accounts_base table is needed for the consolidator's left_anti
# exclusion. It is NOT in the dir_list config. Add its (name, id, path, key, value)
# once you have it; otherwise the script runs WITHOUT the anti-join and warns you.
REPORTABLE_ACCOUNTS_BASE = None
# Example:
# REPORTABLE_ACCOUNTS_BASE = (
#     "enterprise_credit_bureau_reporting_card_metro2_reportable_accounts_base",
#     "<dataset_id>",
#     "s3a://.../enterprise_credit_bureau_reporting_card_metro2_reportable_accounts_base/src",
#     "instnc_id", INSTNC,
# )

# =============================================================================
# 3. Proxy / Spark env setup  [copied from dfs_ol_read_qa.py]
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
# 4. Credential + Spark helpers  [copied from dfs_ol_read_qa.py]
# =============================================================================
def get_oauth_token() -> str:
    """Fetch a fresh client-credentials OAuth token."""
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


def build_spark(access_key: str, secret_key: str, session_token: str) -> SparkSession:
    return (
        SparkSession.builder.appName("reportable_account_count")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.session.token", session_token)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.jars", f"{hadoop_aws_jar},{aws_java_sdk_jar}")
        .getOrCreate()
    )


def reconfigure_s3_creds(spark, access_key, secret_key, session_token):
    """Each dataset has its OWN temporary creds, so refresh the hadoop conf per read."""
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.access.key", access_key)
    hconf.set("fs.s3a.secret.key", secret_key)
    hconf.set("fs.s3a.session.token", session_token)


# =============================================================================
# 5. Load one dataset at its own partition key/value
# =============================================================================
def load_dataset(spark, oauth_token, name, dataset_id, src_path, partition_key, partition_value):
    access_key, secret_key, session_token = get_aws_credentials(dataset_id, oauth_token)
    reconfigure_s3_creds(spark, access_key, secret_key, session_token)

    df = spark.read.format("delta").load(src_path)  # <-- VERIFY: delta vs parquet
    if partition_value is not None:
        df = df.filter(F.col(partition_key) == partition_value)

    cnt = df.count()
    logger.info(f"Loaded {name} @ {partition_key}={partition_value}: {cnt} rows")
    return df


# =============================================================================
# 6. Main: load all datasets, replicate consolidate.py, count reportable accounts
# =============================================================================
def main():
    oauth_token = get_oauth_token()
    logger.info("OAuth token obtained.")

    # Build spark once using the first dataset's creds; refresh creds per read.
    first_id = DATASETS[0][1]
    ak, sk, st = get_aws_credentials(first_id, oauth_token)
    spark = build_spark(ak, sk, st)
    logger.info("Spark session built.")

    # ---- Load every dataset into a dict keyed by name ----
    dfs = {}
    for name, dataset_id, src_path, pkey, pval in DATASETS:
        dfs[name] = load_dataset(spark, oauth_token, name, dataset_id, src_path, pkey, pval)

    # Friendly handles (match consolidate.py variable roles)
    account_service_customer_df = dfs["account_service_customer"]
    account_service_account_df = dfs["account_service_account"]
    credit_bureau_customer_df = dfs["credit_bureau_reporting_service_credit_bureau_customer_dm_os"]
    account_service_address_df = dfs["account_service_address"]
    collector_account_link_df = dfs["collector_service_account_link"]
    collector_config_df = dfs["collector_service_collector_configuration_dm_os"]
    credit_bureau_account_df = dfs["credit_bureau_reporting_service_credit_bureau_account_dm_os"]
    # consolidated_transactions / transaction_history only add COLUMNS, not account
    # filtering, so they are not required for the ACCOUNT COUNT and are omitted here.

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
        b_name, b_id, b_path, b_key, b_val = REPORTABLE_ACCOUNTS_BASE
        base_df = load_dataset(spark, oauth_token, b_name, b_id, b_path, b_key, b_val)
        joined_df = joined_df.join(
            base_df.alias("base"),
            on=F.col("acct.account_id") == F.col("base.account_id"),
            how="left_anti",
        )
        logger.info("Applied left_anti join with metro2 reportable_accounts_base.")
    else:
        logger.warning(
            "REPORTABLE_ACCOUNTS_BASE not set -- skipping the left_anti exclusion. "
            "Count is the PRE-exclusion consolidated population, NOT the final "
            "reportable count. Add the base table config for an exact match."
        )

    # ---- PRIMARY_FILTERS (consolidate.py line 112) ----
    # Known primary filter is cust_role_cd in ('PRIMARY','SECONDARY').
    # If filter_config.PRIMARY_FILTERS has more conditions, ADD them here.
    joined_df = joined_df.filter(
        F.col("cust.cust_role_cd").isin("PRIMARY", "SECONDARY")
    )

    # ---- Final reportable count (consolidate.py line 162) ----
    final_count = joined_df.select(F.col("cust.account_id")).distinct().count()
    logger.info("=" * 60)
    logger.info(f"instnc_id date          : {INSTNC}")
    logger.info(f"load_partition_date     : {LOAD_PART}")
    logger.info(f"Reportable account count: {final_count}")
    logger.info("=" * 60)
    print(f"\nReportable account count (instnc_id={INSTNC}): {final_count}\n")

    spark.stop()
    return final_count


if __name__ == "__main__":
    main()
