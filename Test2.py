
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def test_generic_exception():
    # Define a valid schema
    schema = StructType([
        StructField("account_id", IntegerType(), True),
        StructField("PIF Notification", IntegerType(), True),
        StructField("SIF Notification", IntegerType(), True),
        StructField("Asset Sales Notification", IntegerType(), True),
        StructField("Charge Off Reason Code", StringType(), True),
        StructField("Current Balance of the Account", IntegerType(), True),
        StructField("Bankruptcy Status", StringType(), True),
        StructField("Bankruptcy Chapter", StringType(), True)
    ])
    
    # Provide data that matches the schema but introduces an unexpected failure
    test_data = [
        [1, 1, 0, 0, "STL", 100, "Open", "BANKRUPTCY_CHAPTER_13"],  # Valid row
        [2, "INVALID", 0, 0, "STL", 100, "Open", "BANKRUPTCY_CHAPTER_13"]  # Invalid type for PIF Notification
    ]
    
    input_df = spark.createDataFrame(test_data, schema)
    
    # Use pytest to assert that a generic exception is raised
    with pytest.raises(Exception) as exc_info:
        calculate_current_balance(input_df)  # This should raise an exception due to invalid data type
    
    # Assert the exception is the expected one
    assert "unexpected error occurred" in str(exc_info.value).lower()
    print("Generic exception test case passed!")
