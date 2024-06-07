
import pandas as pd

# Assuming df1 and df2 are already loaded DataFrames with the 'account_id' and 'value' columns properly set
# For example:
# df1 = pd.DataFrame({
#     'account_id': [1, 2, 3],
#     'value': [100, 200, 300]
# })
# df2 = pd.DataFrame({
#     'account_id': [1, 2, 3],
#     'value': [110, 210, 310]
# })

# Merge the DataFrames on 'account_id'
final_df = df1.merge(df2, on='account_id', suffixes=('_original', '_manipulated'))

# Display the final DataFrame
print(final_df)
