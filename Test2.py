
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def test_blank_credit_limit(spark):
    data = [
        ("",  "small_business", 50, None),
        ("  ", "random_type",   51, None),
    ]

    schema = StructType([
        StructField("credit_limit", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("account_id",   IntegerType(), True),
        StructField("expected_account_type", StringType(), True),
    ])

    df = spark.createDataFrame(data, schema=schema)
    result_df = calculate_account_type(df)
    # ... do your assertions or _check_results ...



def test_blank_credit_limit(spark):
    """
    If assigned credit limit is an empty string (after trim), 
    we should assign NULL per the new table.
    """
    data = [
        ("", "small_business",  50, None),
        ("  ", "random_type",   51, None),  # if you consider trimmed whitespace also "blank"
    ]
    # Optional: same schema as your other tests
    # Then run _check_results(data, spark) or do the join-based approach.
    _check_results(data, spark)



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
