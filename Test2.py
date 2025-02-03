from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, trim

def calculate_account_type(input_df: DataFrame) -> DataFrame:
    # No extra column—just trim inline:
    account_type_col = (
        when(trim(col("credit_limit")) == "", None)
        .when(
            (col("product_type") == "private_label_partnership") & 
            (trim(col("credit_limit")) == "NPSL"),
            None
        )
        .when(trim(col("credit_limit")) == "NPSL", "0G")
        .when(col("product_type") == "small_business", "8A")
        .when(col("product_type") == "private_label_partnership", "07")
        .otherwise("18")
    )

    # Keep only the columns you need
    return input_df.select(
        "account_id",
        account_type_col.alias("account_type")
    )
