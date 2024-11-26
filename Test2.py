
from pyspark.sql.functions import col, when, max

def calculate_highest_credit_per_account(df):
    df = df.withColumn('credit_utilized', 
                       when(col('credit_utilized').cast("integer").isNull() | 
                            (col('credit_utilized').cast("integer") < 0), 0)
                       .otherwise(col('credit_utilized').cast("integer")))

    df_filtered = df.filter(~col('is_charged_off'))
    df_max_credit = df_filtered.groupBy('account_id').agg(max('credit_utilized').alias('highest_credit'))
    return df_max_credit


expected_data = [
    (2, 0),  # Non-integer handled as zero
    (3, 0),  # Null value handled as zero
    (4, 0)   # Negative value treated as zero
]


print("Actual Schema:", result_df.schema)
print("Expected Schema:", expected_df.schema)
print("Actual Data:", result_df.collect())
print("Expected Data:", expected_df.collect())

