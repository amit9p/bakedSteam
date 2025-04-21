
field_account_dict = {}

# Loop through failed rules
for rule in entry.get("ruleResults", []):
    if rule.get("result") == "FAIL":
        for failing_data in rule.get("failingRuleSampleData", []):
            data_list = failing_data.get("data", [])
            
            field_name = None
            account_id = None

            for d in data_list:
                if d.get("fieldName") == "account_id":
                    account_id = d.get("value")
                else:
                    field_name = d.get("fieldName")
            
            if field_name and account_id:
                if field_name not in field_account_dict:
                    field_account_dict[field_name] = []
                field_account_dict[field_name].append(account_id)

# Print the dictionary
print(field_account_dict)
