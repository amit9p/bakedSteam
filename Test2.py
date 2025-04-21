
import requests
import json

# Setup API request
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json;v=1",
    "Authorization": "Bearer " + str(oauthToken)
}
url = base_url + result_api_url + "?jobId=" + job_id + "&datasetConfigurationId=" + datasetConfigurationId

# Make API call
api_call_response = requests.get(url, headers=headers, verify=False)
data = json.loads(api_call_response.text)

# Initialize final output dict
field_account_dict = {}

# Work on the first response entry
entry = data[0]
rules = entry.get("ruleResults", [])

for rule in rules:
    if rule.get("result") == "FAIL":
        field_name = rule.get("fieldName")  # e.g. "ssn", "state_code", etc.

        # Safely get the failing sample structure
        failing_samples = rule.get("failingRuleSampleData", {})

        # Extract data blocks (should be a list of lists of dicts)
        data_blocks = failing_samples.get("data", [])

        for data_list in data_blocks:
            for d in data_list:
                if d.get("fieldName") == "account_id":
                    account_id = d.get("value")

                    if field_name not in field_account_dict:
                        field_account_dict[field_name] = []

                    field_account_dict[field_name].append(account_id)

# Output result
print(field_account_dict)
