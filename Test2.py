

from pyspark.sql import SparkSession, functions as F

bucket = "cof-uscard-lossmitigation-cat3-qa-useast1"
prefix = "okes2/lake/recoveries/account_service_account/src/"

spark = (SparkSession.builder
    .appName("CSV sample writer")
    # give the driver more RAM
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.files.maxPartitionBytes", "64m")
    # make reads lighter (less memory hungry)
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate())

path = f"s3a://{bucket}/{prefix}"

# 1) Read only a handful of files (uses glob). Adjust pattern to match your files.
#    If you know a specific small file, give its full path instead of the glob.
df_fs = (spark.read
         .option("recursiveFileLookup", "true")
         .option("pathGlobFilter", "part-0000*.parquet")  # <- read just a few files
         .parquet(path))

# (Optional) Select only columns you need; fewer columns = less memory
NEEDED = ["account_id", "instnc_id", "first_name", "last_name"]  # adapt to your schema
df_fs = df_fs.select(*NEEDED)

# Fix 32-bit vs 64-bit ints if needed
df_fs = (df_fs
         .withColumn("account_id", F.col("account_id").cast("long"))
         .withColumn("instnc_id",  F.col("instnc_id").cast("long")))







from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("read with schema merge")
    .config("spark.sql.parquet.mergeSchema", "true")   # promote mixed int32/int64 to long
    .getOrCreate()
)

# also add the option on the reader (belt & suspenders)
df_fs = spark.read.option("mergeSchema", "true").parquet(fs_filepath)

# Make sure “problem” ints are long so CSV write won’t choke
int_maybe64 = ["account_id", "instnc_id"]     # add more if needed
df_fs = df_fs.select(*[
    F.col(c).cast("long") if c in int_maybe64 else F.col(c) for c in df_fs.columns
])

# If you only need 1000 rows:
df_1000 = df_fs.limit(1000)

# Write CSV (single file, with header)
out = "file:///Users/…/account_service_account"
df_1000.coalesce(1).write.mode("overwrite").option("header", "true").csv(out)


from pyspark.sql import functions as F, types as T

int32_cols = ["account_id", "instnc_id"]  # add any others that must be 32-bit
df = df.withColumns({c: F.col(c).cast(T.IntegerType()) for c in int32_cols})

nc -vz <phone_ip> 8080       # should say 'succeeded'
curl -I --proxy http://<phone_ip>:8080 http://example.com
curl -I --proxy http://<phone_ip>:8080 https://example.com






Your current names vs. fixed names

Current name	Suggested name

ECBR_Calculator_Output	EcbrCalculatorOutput
ECBR_Consolidator_Account_Service_Account	EcbrConsolidatorAccountServiceAccount
ECBR_Consolidator_Account_Service_Address	EcbrConsolidatorAccountServiceAddress
ECBR_Consolidator_Account_Service_Customer	EcbrConsolidatorAccountServiceCustomer
ECBR_Consolidator_Credit_Bureau_Account_Dm_Os	EcbrConsolidatorCreditBureauAccountDmOs
ECBR_Consolidator_Customer_Dm_Os	EcbrConsolidatorCustomerDmOs
ECBR_Consolidator_Service_Collector_Configuration	EcbrConsolidatorServiceCollectorConfiguration
ECBR_Consolidator_Trxn_Service_Full_Extract	EcbrConsolidatorTrxnServiceFullExtract



---

How to apply changes safely

1. Rename class definitions in your schema files (change the class keyword).
Example:

# before
class ECBR_Calculator_Output(Schema):
    ...

# after
class EcbrCalculatorOutput(Schema):
    ...


2. Update all imports across your repo (tests, consolidators, etc.).
Example:

from schemas.ecbr_calculator_dfs_output import EcbrCalculatorOutput


3. Run tests again (pipenv run pytest -q) to confirm everything still works.








import pytest
from typedspark import create_schema

from ecbr_tenant_card_dfs_l1_self_service.schemas.ecbr_consolidator_dfs_account_service_account import (
    ECBR_Consolidator_Account_Service_Account,
)
from ecbr_tenant_card_dfs_l1_self_service.schemas.ecbr_calculator_dfs_output import (
    ECBR_Calculator_Output,
)

CANDIDATES = [
    ECBR_Consolidator_Account_Service_Account,
    ECBR_Calculator_Dfs_Output,   # add others as needed
]

def test_build_all_schemas():
    built = 0
    failures = {}
    for cls in CANDIDATES:
        try:
            create_schema(cls)
            built += 1
        except Exception as e:
            failures[cls.__name__] = str(e)

    if built == 0:
        pytest.skip(f"No typed-spark schemas built; failures={failures}")
