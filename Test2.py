
from pyspark.sql import functions as F

bad_balance_df = df.filter(
    (F.col("attribute") == "balance_amt") &
    (
        F.col("formatted").isNull() |
        (F.trim(F.col("formatted")) == "") |
        (~F.col("formatted").rlike("^[0-9]+$")) |
        (F.length(F.col("formatted")) != 9)
    )
).select(
    "run_id",
    "account_number",
    "segment",
    "attribute",
    "formatted",
    F.length("formatted").alias("formatted_length")
)

bad_balance_df.show(100, False)
