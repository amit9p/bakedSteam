
import requests
import json

# Make the API call
api_call_response = requests.get(url, headers=headers, verify=False)
data = json.loads(api_call_response.text)

# Initialize an empty dictionary to store account_id values by fieldName
field_account_dict = {}

# Extract failed rules from the first item
entry = data[0]
rules = entry.get("ruleResults", [])

# Iterate over the rules to find failed ones and store account_ids
for rule in rules:
    if rule.get("result") == "FAIL":
        # Iterate over the failing data
        for failing_data in rule.get("failingRuleSampleData", []):
            field_name = failing_data.get('fieldName')
            account_id = failing_data.get('value')  # Assuming the account_id is stored in 'value'
            
            # Initialize the list for the fieldName if it's not already in the dictionary
            if field_name not in field_account_dict:
                field_account_dict[field_name] = []
            
            # Append the account_id to the list of the corresponding fieldName
            field_account_dict[field_name].append(account_id)

# Print the resulting dictionary
print(field_account_dict)
