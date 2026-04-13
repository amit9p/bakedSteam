
from pyspark.sql.functions import broadcast

reportable_accounts_df = reportable_accounts_df.join(
    broadcast(edq_accounts_df),
    "account_id",
    "left_anti"
)


class TestReportableAccounts:

    def test_without_edq(self, calculated_df, consolidated_df):
        result_df = get_reportable_accounts(
            calculated_dataset=calculated_df,
            consolidated_dataset=consolidated_df
        )

        assert result_df.count() > 0


    def test_edq_suppression(self, calculated_df, consolidated_df, edq_suppressions_df):
        result_df = get_reportable_accounts(
            calculated_dataset=calculated_df,
            consolidated_dataset=consolidated_df,
            edq_suppressions_df=edq_suppressions_df
        )

        result_accounts = [r.account_id for r in result_df.collect()]

        assert "2" not in result_accounts



@pytest.fixture
def edq_suppressions_df(spark):
    """
    Provides EDQ suppression DataFrame.
    """
    data = [
        {"account_id": "2"},   # this account should get suppressed
    ]

    return spark.createDataFrame(data)



@pytest.fixture
def empty_edq_df(spark):
    return spark.createDataFrame([], "account_id string")




def test_without_edq(calculated_df, consolidated_df):
    result_df = get_reportable_accounts(
        calculated_dataset=calculated_df,
        consolidated_dataset=consolidated_df
    )

    assert result_df.count() > 0



def test_edq_suppression(
    calculated_df,
    consolidated_df,
    edq_suppressions_df
):
    result_df = get_reportable_accounts(
        calculated_dataset=calculated_df,
        consolidated_dataset=consolidated_df,
        edq_suppressions_df=edq_suppressions_df
    )

    result_accounts = [row.account_id for row in result_df.collect()]

    # account_id = "2" should be removed
    assert "2" not in result_accounts




def test_empty_edq(
    calculated_df,
    consolidated_df,
    empty_edq_df
):
    result_df = get_reportable_accounts(
        calculated_dataset=calculated_df,
        consolidated_dataset=consolidated_df,
        edq_suppressions_df=empty_edq_df
    )

    assert result_df.count() > 0



def test_edq_reduces_count(
    calculated_df,
    consolidated_df,
    edq_suppressions_df
):
    df_without = get_reportable_accounts(
        calculated_dataset=calculated_df,
        consolidated_dataset=consolidated_df
    )

    df_with = get_reportable_accounts(
        calculated_dataset=calculated_df,
        consolidated_dataset=consolidated_df,
        edq_suppressions_df=edq_suppressions_df
    )

    assert df_with.count() < df_without.count()






__________________
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

