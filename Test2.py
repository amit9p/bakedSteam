
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
