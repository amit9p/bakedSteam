
from pathlib import Path

# Get the current script directory
current_dir = Path(__file__).resolve().parent

# Move up one level to reach 'edq' directory
edq_dir = current_dir.parent

# Construct the 'rules' directory path using '..'
rules_dir = edq_dir / "edq" / "rules"

print("Rules Directory:", rules_dir)

# Example of constructing JSON file paths dynamically
def get_json_file_path(field_name):
    """Returns the full path of a JSON file in the 'rules' directory."""
    file_path = rules_dir / f"{field_name}.json"
    return file_path if file_path.exists() else None

# Example Usage
json_filename = "portfolio_type"
full_path = get_json_file_path(json_filename)

if full_path:
    print("Full path to JSON file:", full_path)
else:
    print(f"File {json_filename}.json not found in rules directory")



import os
from pathlib import Path

# Get the current working directory (where edq_rule_engine.py is located)
current_dir = Path(__file__).resolve().parent  # Path of edq_rule_engine.py

# Move up one level to reach 'edq' directory
edq_dir = current_dir.parent

# Construct full path to the 'rules' folder
rules_dir = edq_dir / "rules"

def get_json_file_path(filename):
    """Constructs the full path for a given JSON file in the 'rules' directory."""
    file_path = rules_dir / filename
    if file_path.exists():
        return str(file_path)
    else:
        raise FileNotFoundError(f"File not found: {file_path}")

# Example Usage
json_filename = "first_name.json"
full_path = get_json_file_path(json_filename)

print("Full path to JSON file:", full_path)



def parse_arguments():
    parser = argparse.ArgumentParser(
        description="1. Path to the input JSON file with information to create ruleset, dataset, job, and rules\n"
                    "2. Environment to operate on: nonprod or prod\n"
                    "3. The set of rules which the job is created with\n"
                    "4. Field names to be processed (one or more)"
    )

    parser.add_argument("--input_json", type=str, required=True, help="Path to the input JSON file")
    parser.add_argument("--env", type=str, required=True, choices=['nonprod', 'prod'], help="Environment to operate on")
    parser.add_argument("--rule_type", type=str, required=True, help="The set of rules which the job is created with")
    parser.add_argument("--field_name", type=str, nargs='+', required=True, help="One or more field names to be processed")

    return parser.parse_args()




def main():
    # Parse arguments
    args = parse_arguments()

    # Extract individual arguments
    input_json_path = args.input_json
    env = args.env
    rule_type = args.rule_type
    field_names = args.field_name  # This is a list of field names

    # Print or use field names in further processing
    print("Input JSON:", input_json_path)
    print("Environment:", env)
    print("Rule Type:", rule_type)
    print("Field Names:", field_names)  # This will print all provided field names as a list

    # Example: Processing each field
    for field in field_names:
        print(f"Processing field: {field}")

# Run main function
if __name__ == "__main__":
    main()





import pytest
import json
import requests
from unittest.mock import patch, mock_open
from your_script import (  # Replace `your_script` with your actual script filename
    load_json_input,
    parse_arguments,
    get_access_token,
    create_rules,
    main,
)

# Sample test data
SAMPLE_JSON = '{"job_id": "123", "rules": [{"ruleName": "Rule1"}]}'
MOCK_ENV_URLS = {"nonprod": "https://api-tst.cloud.capitalone.com", "prod": "https://api.cloud.capitalone.com"}
MOCK_ACCESS_TOKEN = "mocked_access_token"
MOCK_HEADERS = {"Authorization": f"Bearer {MOCK_ACCESS_TOKEN}"}


# Test load_json_input
def test_load_json_input():
    with patch("builtins.open", mock_open(read_data=SAMPLE_JSON)):
        data = load_json_input("dummy_path.json")
    assert isinstance(data, dict)
    assert data["job_id"] == "123"


# Test parse_arguments (mocking argparse)
def test_parse_arguments():
    test_args = ["--input_json", "test.json", "--env", "nonprod", "--rule_type", "default"]
    with patch("sys.argv", ["script_name"] + test_args):
        args = parse_arguments()
    assert args.input_json == "test.json"
    assert args.env == "nonprod"
    assert args.rule_type == "default"


# Test get_access_token (mocking requests)
@patch("requests.post")
def test_get_access_token(mock_post):
    mock_response = {"access_token": MOCK_ACCESS_TOKEN}
    mock_post.return_value.json.return_value = mock_response
    mock_post.return_value.raise_for_status = lambda: None  # No exception

    token = get_access_token("client_id", "client_secret", "nonprod")
    assert token == MOCK_ACCESS_TOKEN
    mock_post.assert_called_once()


# Test create_rules (mocking requests)
@patch("requests.post")
def test_create_rules(mock_post):
    job_id = "123"
    rule_list = [{"ruleName": "Rule1"}]
    base_url = MOCK_ENV_URLS["nonprod"]

    mock_response = {"successfulRuleList": rule_list, "failedRuleList": []}
    mock_post.return_value.json.return_value = mock_response
    mock_post.return_value.raise_for_status = lambda: None

    create_rules(job_id, rule_list, MOCK_HEADERS, base_url)
    mock_post.assert_called_once()


# Test main function (mocking API calls and file reading)
@patch("requests.post")
@patch("builtins.open", mock_open(read_data=SAMPLE_JSON))
def test_main(mock_post):
    mock_response = {"access_token": MOCK_ACCESS_TOKEN}
    mock_post.return_value.json.return_value = mock_response
    mock_post.return_value.raise_for_status = lambda: None

    with patch("sys.argv", ["script_name", "--input_json", "test.json", "--env", "nonprod", "--rule_type", "default"]):
        main()  # Run main()

    mock_post.assert_called()


if __name__ == "__main__":
    pytest.main()
