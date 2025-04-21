
field_account_dict = {}

entry = data[0]  # assuming data is a list with one main entry
rules = entry.get("ruleResults", [])

for rule in rules:
    if rule.get("result") == "FAIL":
        field_name = rule.get("fieldName")
        
        # Loop through each sample block
        for failing_sample in rule.get("failingRuleSampleData", []):
            data_list = failing_sample.get("data", [])
            
            # Loop through data points and collect account_ids
            for d in data_list:
                if d.get("fieldName") == "account_id":
                    account_id = d.get("value")
                    
                    if field_name not in field_account_dict:
                        field_account_dict[field_name] = []
                    
                    field_account_dict[field_name].append(account_id)

# Print the final dictionary
print(field_account_dict)
