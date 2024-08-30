
from pyspark.sql import functions as F

# Assuming df_A is the DataFrame for table A and df_B is the DataFrame for table B
# Load your DataFrames here (df_A and df_B)

# Perform a left join on tokenization, account_number, and segment
df_joined = df_A.alias('A').join(
    df_B.alias('B'),
    on=[
        df_A.tokenization == df_B.tokenization,
        df_A.account_number == df_B.account_number,
        df_A.segment == df_B.segment
    ],
    how='left'
)

# Use coalesce to replace 'formatted' with 'formatted' from table B if exists, otherwise keep original
df_result = df_joined.withColumn(
    'formatted',
    F.coalesce(df_B.formatted, df_A.formatted)
).select(df_A['*'])  # Selecting all columns from A (excluding duplicated columns from B)

# Show the resulting DataFrame
df_result.show()
