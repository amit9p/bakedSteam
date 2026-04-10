
def get_reportable_accounts(
    calculated_dataset: DataFrame,
    consolidated_dataset: DataFrame,
    edq_suppressions_df: DataFrame = None,   # 👈 ADD THIS
    context: dict = None,
) -> DataFrame:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession()

if edq_suppressions_df is None:
    edq_suppressions_df = spark.createDataFrame([], "account_id string")
