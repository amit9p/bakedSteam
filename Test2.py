

from pyspark.sql import functions as F

df_updated = df_input.withColumn("account_id", F.lit("1234"))

(
    df_updated
    .coalesce(1)                         # only 1 output file
    .write
    .mode("overwrite")
    .parquet("file:///tmp/output_parquet")
)

from pyspark.sql import functions as F

# df_input has a column "account_id"

# 1. Create a new DF with a modified value
# Example: prefix "ACC_" to account_id
df_new = (
    df_input
    .withColumn("new_account_id", F.concat(F.lit("ACC_"), F.col("account_id")))
    .select("new_account_id")
)

# 2. Write as a single parquet file to local folder
(
    df_new
    .coalesce(1)       # make 1 output parquet file
    .write
    .mode("overwrite")
    .parquet("file:///tmp/output_account_ids")   # local path
)

print("Parquet written successfully")

"Access is required to read Omega input datasets used by the DFSL1 Card Data Ingest pipeline. This access is needed to validate source data, perform development and testing activities, and ensure accurate processing of reportable, consolidator, and calculator account datasets for the DFSL1 workflow."

Hi
Our file puller for the joiner_output dataset failed because the output now includes the customer_id field, but the current Exchange registration doesn’t list it.

We already have enterprise_servicing_customer_id in Exchange, but for this dataset, we need to add customer_id as well to align with the joiner output schema.

Can you please confirm if it’s okay to update the registration to include this column? Once confirmed, I’ll make the change and rerun the test to validate.

Thanks!
