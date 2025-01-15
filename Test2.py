
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

# Define the schema once
schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("PIF Notification", BooleanType(), True),
    StructField("SIF Notification", BooleanType(), True),
    StructField("Asset Sales Notification", BooleanType(), True),
    StructField("Charge Off Reason Code", StringType(), True),
    StructField("Current Balance of the Account", IntegerType(), True),
    StructField("Bankruptcy Status", StringType(), True),
    StructField("Bankruptcy Chapter", StringType(), True),
])

# Positive Test Case
def test_positive_case():
    test_data = [
        (1, True, False, False, "STL", 100, "Open", "BANKRUPTCY_CHAPTER_7"),
        (2, False, True, False, "BD", -200, "Open", "BANKRUPTCY_CHAPTER_11"),
    ]
    input_df = spark.createDataFrame(test_data, schema)
    result = calculate_current_balance(input_df)
    assert result.collect() == [(1, 0), (2, 0)]
    print("Positive test case passed!")

# Negative Test Case
def test_negative_case():
    test_data = [
        (3, False, False, False, "BD", 300, "Closed", "BANKRUPTCY_CHAPTER_7"),
        (4, False, False, False, "BD", 400, "Open", "BANKRUPTCY_CHAPTER_11"),
    ]
    input_df = spark.createDataFrame(test_data, schema)
    result = calculate_current_balance(input_df)
    assert result.collect() == [(3, 300), (4, 400)]
    print("Negative test case passed!")

# Edge Test Case
def test_edge_case():
    test_data = [
        (5, False, True, False, "STL", 0, "Open", "BANKRUPTCY_CHAPTER_13"),
    ]
    input_df = spark.createDataFrame(test_data, schema)
    result = calculate_current_balance(input_df)
    assert result.collect() == [(5, 0)]
    print("Edge test case passed!")
