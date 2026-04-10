

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("parquet_to_single_file").getOrCreate()

# Read local parquet file/folder
input_path = "/path/to/input_parquet"

df = spark.read.parquet(input_path)

# Write as single output file
output_path = "/path/to/output_folder"

df.coalesce(1).write.mode("overwrite").parquet(output_path)

spark.stop()
__________
def get_reportable_accounts(
    calculated_dataset: DataFrame,
    consolidated_dataset: DataFrame,
    edq_suppressions_df: DataFrame = None,   # 👈 ADD THIS
    context: dict = None,
) -> DataFrame:
--------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()

if edq_suppressions_df is None:
    edq_suppressions_df = spark.createDataFrame([], "account_id string")


______________


# --- STEP 6.5: Apply EDQ Suppression (FINAL FILTER) ---

edq_accounts_df = (
    edq_suppressions_df
    .select("account_id")
    .dropDuplicates()
    .withColumn("is_edq_suppressed", F.lit(True))
)

reportable_accounts_df = reportable_accounts_df.join(
    edq_accounts_df,
    on="account_id",
    how="left"
).filter(
    F.col("is_edq_suppressed").isNull()
)

-----------

