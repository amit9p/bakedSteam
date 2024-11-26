
from pyspark.sql.functions import col, when, max
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

def calculate_highest_credit_per_account(df):
    # Add a column that attempts to cast credit_utilized to integer, defaults to 0 on failure
    df = df.withColumn('credit_utilized_int', 
                       when(col('credit_utilized').cast("integer").isNull(), 0)
                       .otherwise(col('credit_utilized').cast("integer")))

    # Handle negatives and nulls by ensuring all values are non-negative integers
    df = df.withColumn('credit_utilized_clean',
                       when(col('credit_utilized_int') < 0, 0)
                       .otherwise(col('credit_utilized_int')))

    df.show()  # Debug: Show the DataFrame after cleanup

    # Filter out rows where 'is_charged_off' is true
    df_filtered = df.filter(~col('is_charged_off'))

    # Calculate the maximum credit utilized for each account
    df_max_credit = df_filtered.groupBy('account_id').agg(max('credit_utilized_clean').alias('highest_credit'))

    df_max_credit.show()  # Debug: Show the resulting aggregation

    return df_max_credit

# Example usage:
spark = SparkSession.builder.master("local").appName("Example").getOrCreate()
data = [(1, '500', False), (2, 'invalid', False), (3, None, False), (4, '-100', False)]
schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("credit_utilized", StringType(), True),
    StructField("is_charged_off", BooleanType(), True)
])

df = spark.createDataFrame(data, schema=schema)
result_df = calculate_highest_credit_per_account(df)
