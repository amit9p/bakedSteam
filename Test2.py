
for field, account_ids in result_api.items():
    print(f"\n--- Records for field: {field} ---")
    
    # Remove duplicates from account_ids
    unique_ids = list(set(account_ids))
    
    base_df.select("account_id", field) \
        .filter(base_df["account_id"].isin(unique_ids)) \
        .distinct() \
        .show(truncate=False)
