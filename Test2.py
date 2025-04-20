
import requests
import json

# Call API
response = requests.get(url, headers=headers, verify=False)
data = json.loads(response.text)

# Extract failed rules
for entry in data:
    rules = entry.get("rulesResults", [])
    for rule in rules:
        if rule.get("result") == "FAIL":
            print(f"Rule ID: {rule.get('ruleId')}, Execution Time: {rule.get('executionTime')}")
