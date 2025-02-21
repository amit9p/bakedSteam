

> "The data itself contains values like 'Open' and 'Discharged' with the first letter capitalized. Since we're matching against the exact data values, there's no need to convert to lowercase. However, if there's a chance of inconsistent casing in future data, we can use .lower() on both sides to ensure case-insensitive comparison."


in the box (^[a-zA-Z]( *[a-zA-Z])*$) is 


from pyspark.sql import Row
import pytest

def test_mismatched_ids(spark):
    """
    Ensures that if 'recoveries_df' or 'customer_df' does NOT have a matching record,
    we still handle NULL columns gracefully.
    """

    account_data = [
        Row(account_id="A100", is_account_paid_in_full=0,
            post_charge_off_account_settled_in_full_notification=0,
            pre_charge_off_account_settled_in_full_notification=0,
            posted_balance=777, last_reported_1099_amount=754),

        Row(account_id="A200", is_account_paid_in_full=0,
            post_charge_off_account_settled_in_full_notification=0,
            pre_charge_off_account_settled_in_full_notification=0,
            posted_balance=666, last_reported_1099_amount=654),
    ]

    rec_data = [
        Row(account_id="A300", asset_sales_notification=0),
        Row(account_id="A400", asset_sales_notification=1),
    ]

    cust_data = [
        Row(account_id="A500", bankruptcy_status="Closed", bankruptcy_chapter="7"),
        Row(account_id="A600", bankruptcy_status="Open", bankruptcy_chapter="13"),
    ]

    account_df = spark.createDataFrame(account_data)
    rec_df = spark.createDataFrame(rec_data)
    cust_df = spark.createDataFrame(cust_data)

    result_df = calculate_current_balance(account_df, rec_df, cust_df)

    # ✅ Define the expected DataFrame for full assertion
    expected_data = [
        Row(account_id="A100", current_balance_amount=23),  # 777 - 754
        Row(account_id="A200", current_balance_amount=12),  # 666 - 654
    ]

    expected_df = spark.createDataFrame(expected_data)

    # ✅ Assert Full DataFrame Equality
    assert result_df.exceptAll(expected_df).isEmpty() and expected_df.exceptAll(result_df).isEmpty(), "DataFrames do not match!"

    print("Test passed: Full DataFrame assertion successful!")
