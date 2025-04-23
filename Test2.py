
def extract_primary_keys_by_field(df, field_list):
    result = {}
    for field in field_list:
        keys = (df.filter(df.field_name == field)
                  .select("primary_key")
                  .distinct()
                  .rdd.flatMap(lambda x: x)
                  .collect())
        result[field] = keys
    return result



fields = ["scheduled_payment_amount", "consumer_account_number"]
output = extract_primary_keys_by_field(your_dataframe, fields)
print(output)
