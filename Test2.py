
def scheduled_payment_amount(df: DataFrame) -> DataFrame:
    """
    Ensures the `scheduled_payment_amount` column is always set to 0 for charged-off customers.

    :param df: Input DataFrame with `account_id` and `scheduled_payment_amount` columns.
    :return: DataFrame with `scheduled_payment_amount` set to 0.
    """
    # Validate column existence
    required_columns = {"account_id", "scheduled_payment_amount"}
    if not required_columns.issubset(df.columns):
        raise ValueError(f"Input DataFrame must contain columns: {required_columns}")
    
    # Set `scheduled_payment_amount` to 0 for all rows
    result_df = df.withColumn("scheduled_payment_amount", lit(0))
    
    return result_df.select("account_id", "scheduled_payment_amount")
