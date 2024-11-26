
def calculate_highest_credit_per_account(df):
    """
    Calculates the highest credit amount utilized per account, handling errors for non-integer, null, or negative values.
    
    :param df: DataFrame containing account history.
    :return: DataFrame with highest credit utilized per account.
    """
    df = df.withColumn('credit_utilized', 
                       when(col('credit_utilized').cast("integer").isNull() | (col('credit_utilized') < 0), 0)
                       .otherwise(col('credit_utilized').cast("integer")))

    df_filtered = df.filter(~col('is_charged_off'))

    df_max_credit = df_filtered.groupBy('account_id').agg(max('credit_utilized').alias('highest_credit'))

    return df_max_credit




