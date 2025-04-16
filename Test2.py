
from pyspark.sql import SparkSession

# Sample list of account IDs to filter
account_ids = ['A1001', 'A1003', 'A1005']

# Assuming df is your DataFrame and it has a column named 'account_id'
filtered_df = df.filter(df['account_id'].isin(account_ids))

# Show the result
filtered_df.show()
