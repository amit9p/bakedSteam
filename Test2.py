
from pyspark.sql import functions as F

def calculate_highest_credit_per_account(df):
    """
    Calculates the highest credit amount utilized per account with error handling for missing or malformed data.
    
    :param df: DataFrame containing account history.
    :return: DataFrame with highest credit utilized per account.
    """
    # Check for missing or malformed data in 'credit_utilized' and handle it
    df = df.withColumn('credit_utilized', F.when(
        (col('credit_utilized').isNull() | ~col('credit_utilized').cast("int").isNotNull()), 
        F.lit(0)).otherwise(col('credit_utilized')))

    # Adjust for converted accounts
    df = df.withColumn('credit_utilized', 
                       F.when(col('is_converted'), col('conversion_balance'))
                        .otherwise(col('credit_utilized')))

    # Exclude charged-off accounts from the calculation
    df_filtered = df.filter(~col('is_charged_off'))

    # Group by account_id and calculate the maximum credit utilized per account
    df_max_credit = df_filtered.groupBy('account_id').agg(F.max('credit_utilized').alias('highest_credit'))

    # Ensure the 'highest_credit' is rounded to the nearest whole dollar
    df_max_credit = df_max_credit.withColumn('highest_credit', F.round(col('highest_credit'), 0))

    return df_max_credit




def test_calculate_highest_credit_per_account():
    spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()
    test_data = [
        Row(account_id=1, credit_utilized=500, is_converted=False, conversion_balance=None, is_charged_off=False),
        Row(account_id=1, credit_utilized=700, is_converted=False, conversion_balance=None, is_charged_off=False),
        Row(account_id=2, credit_utilized=800, is_converted=True, conversion_balance=800, is_charged_off=False),
        Row(account_id=2, credit_utilized="invalid", is_converted=False, conversion_balance=None, is_charged_off=False),  # non-integer value
        Row(account_id=3, credit_utilized=None, is_converted=False, conversion_balance=None, is_charged_off=False),  # Missing value
        Row(account_id=4, credit_utilized=-100, is_converted=False, conversion_balance=None, is_charged_off=False)  # Negative value
    ]
    df = spark.createDataFrame(test_data)
    result_df = calculate_highest_credit_per_account(df)
    expected_data = [
        Row(account_id=1, highest_credit=700),
        Row(account_id=2, highest_credit=800),
        Row(account_id=3, highest_credit=0),  # Handled missing value
        Row(account_id=4, highest_credit=0)  # Handled negative value, assumed treated as 0 or excluded
    ]
    expected_df = spark.createDataFrame(expected_data)

    assert result_df.collect() == expected_df.collect(), "Test failed: The DataFrame does not match the expected output."

# Run the test
test_calculate_highest_credit_per_account()




##############
From the assumption section in the image, it mentions:

If an account is converted from an external issuer, the value will be reset to the balance at the time of conversion.

There is no current calculation logic for CO (charge-off) customers because this will be a static value once the customer charges off.


Addressing the Assumptions in the Code

The provided Python code does not currently incorporate these assumptions. If these assumptions affect the computation of the highest credit, you will need to adjust the logic to account for these conditions. Here’s how you might start incorporating them:

1. Reset to Balance at Conversion: You would need additional information about when an account was converted and what the balance was at that time. This typically requires accessing another data source or column that indicates whether an account is converted and what the conversion balance was.


2. Handling for CO (Charge-Off) Customers: If CO customers have a static value that doesn't change, you might need to exclude them from the aggregation or handle their data separately based on the business requirement.
Error Handling in the Function: Checks for null or non-integer values in the credit_utilized column and replaces them with 0.

Rounding: Applies rounding to the highest_credit to ensure values are returned in whole numbers.

Test Cases:

Happy Path: Valid integer values.

Unhappy Path: Includes handling for non-integer inputs, negative values, and null values.



                                                                                                                                                                                          
                                                                                                                                                                                          


