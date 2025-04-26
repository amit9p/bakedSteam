
import json
import os

# Get path to current directory and the JSON file
current_dir = os.path.dirname(os.path.abspath(__file__))
json_file_path = os.path.join(current_dir, "final_output.json")

# Load each JSON line and print desired fields
with open(json_file_path, "r") as f:
    for line in f:
        record = json.loads(line)
        print("Field Name:", record.get("field_name"))
        print("Account IDs:", record.get("account_id"))
        print("-" * 40)
