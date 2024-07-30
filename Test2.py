
def replace_tokenized_values(input_df, token_cache_df):
    try:
        # Select the necessary columns from token_cache_df
        token_cache_selected_df = token_cache_df.select("account_number", "tokenization", "plain_text").withColumnRenamed(
            "plain_text", "new_formatted"
        )
        
        # Perform the join on 'account_number' and 'tokenization' and select the columns, replacing input_df's value column
        joined_df = input_df.join(token_cache_selected_df, on=["account_number", "tokenization"], how="left")
        
        # Replace the original 'formatted' column with the new 'plain_text' column
        updated_df = joined_df.withColumn(
            "formatted", coalesce(token_cache_selected_df["new_formatted"], input_df["formatted"])
        ).drop("new_formatted")
        
        # Reorder the columns to match the original input_df schema
        original_columns = input_df.columns
        final_df = updated_df.select(original_columns)
        
        return final_df
    except Exception as e:
        logger.error(e)

# Example of the previous test function with renamed variables
def test_replace_tokenized_values_with_empty_cache(spark, input_df):
    # Define the schema explicitly
    schema = StructType([
        StructField("account_number", LongType(), True),
        StructField("tokenization", StringType(), True),
        StructField("plain_text", StringType(), True)
    ])
    empty_token_cache_df = spark.createDataFrame([], schema)
    result_df = replace_tokenized_values(input_df, empty_token_cache_df)
    assert_dataframe_equality(result_df, input_df)
