
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col

def calculate_account_type(input_df: DataFrame) -> DataFrame:
    """
    Simplified logic with no helper column:
      1) If (product_type == "private_label_partnership") AND (credit_limit == "NPSL") => None
      2) Else if (credit_limit == "NPSL") => "0G"
      3) Else if (product_type == "small_business") => "8A"
      4) Else if (product_type == "private_label_partnership") => "07"
      5) Else => "18"
    """

    account_type_col = (
        when(
            (col("product_type") == "private_label_partnership") & 
            (col("credit_limit") == "NPSL"),
            None
        )
        .when(col("credit_limit") == "NPSL", "0G")
        .when(col("product_type") == "small_business", "8A")
        .when(col("product_type") == "private_label_partnership", "07")
        .otherwise("18")
    )

    final_df = input_df.select(
        "account_id",
        account_type_col.alias("account_type")
    )
    return final_df
