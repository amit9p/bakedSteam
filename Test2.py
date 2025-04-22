
# Sample dictionary like in your image
failed_rules_dict = {
    'scheduled_payment_amount': ['r8p3816fKe6', '4', 'Do18Z'],
    'consumer_account_number': ['N2', 'r8p3816fKe6', '4']
}

# Your PySpark DataFrame (replace with actual DataFrame)
# Example:
# df = spark.read.parquet("your_path")

for field, account_ids in failed_rules_dict.items():
    print(f"\n--- Records for field: {field} ---")
    df.filter(df[field].isin(account_ids)).show(truncate=False)
