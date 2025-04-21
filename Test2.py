
print(">>>", failing_sample, type(failing_sample))



field_name = rule.get("fieldName")
print("field_name")
failing_samples = rule.get("failingRuleSampleData", {})

# Just access the 'data' list of lists
data_blocks = failing_samples.get("data", [])

for data_list in data_blocks:  # each item here is a list of dicts
    for d in data_list:
        if d.get("fieldName") == "account_id":
            account_id = d.get("value")

            if field_name not in field_account_dict:
                field_account_dict[field_name] = []

            field_account_dict[field_name].append(account_id)


#÷÷÷÷÷÷









for rule in rules:
    if rule.get("result") == "FAIL":
        field_name = rule.get("fieldName")

        failing_samples = rule.get("failingRuleSampleData", [])
        if isinstance(failing_samples, str):
            # If it's still a string, parse it again
            failing_samples = json.loads(failing_samples)

        for failing_sample in failing_samples:
            data_list = failing_sample.get("data", [])

            for d in data_list:
                if d.get("fieldName") == "account_id":
                    account_id = d.get("value")

                    if field_name not in field_account_dict:
                        field_account_dict[field_name] = []
                    field_account_dict[field_name].append(account_id)


#####


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
