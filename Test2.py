
import pytest
import json
import requests
from unittest.mock import patch, mock_open
from pathlib import Path
from edq_rule_engine import (
    parse_arguments,
    load_json_input,
    get_access_token,
    create_rules,
    main
)

# Sample JSON data for testing
SAMPLE_JSON = '{"job_id": "123", "rules": {"validation": [{"ruleName": "Rule1"}]}}'
MOCK_ACCESS_TOKEN = "mocked_access_token"
MOCK_HEADERS = {"Authorization": f"Bearer {MOCK_ACCESS_TOKEN}"}
MOCK_ENV_URLS = {"nonprod": "https://api-tst.cloud.capitalone.com", "prod": "https://api.cloud.capitalone.com"}

# ========== Test `parse_arguments()` ==========
def test_parse_arguments(monkeypatch):
    test_args = ["script.py", "--input_json", "test.json", "--env", "nonprod", "--rule_type", "validation", "--field_name", "name", "age"]
    monkeypatch.setattr("sys.argv", test_args)
    
    args = parse_arguments()
    assert args.input_json == "test.json"
    assert args.env == "nonprod"
    assert args.rule_type == "validation"
    assert args.field_name == ["name", "age"]

# ========== Test `load_json_input()` ==========
def test_load_json_input():
    with patch("builtins.open", mock_open(read_data=SAMPLE_JSON)):
        data = load_json_input("dummy.json")
    assert isinstance(data, dict)
    assert data["job_id"] == "123"
    assert "rules" in data

# ========== Test `get_access_token()` ==========
@patch("requests.post")
def test_get_access_token(mock_post):
    mock_post.return_value.json.return_value = {"access_token": MOCK_ACCESS_TOKEN}
    mock_post.return_value.raise_for_status = lambda: None  # Simulate successful request

    token = get_access_token("client_id", "client_secret", "nonprod")
    assert token == MOCK_ACCESS_TOKEN
    mock_post.assert_called_once()

# ========== Test `create_rules()` ==========
@patch("requests.post")
def test_create_rules(mock_post):
    job_id = "123"
    rule_list = [{"ruleName": "Rule1"}]
    base_url = MOCK_ENV_URLS["nonprod"]

    mock_response = {"successfulRuleList": rule_list, "failedRuleList": []}
    mock_post.return_value.json.return_value = mock_response
    mock_post.return_value.raise_for_status = lambda: None  # Simulate successful request

    create_rules(job_id, rule_list, MOCK_HEADERS, base_url)
    mock_post.assert_called_once()

# ========== Test `main()` ==========
@patch("requests.post")
@patch("builtins.open", mock_open(read_data=SAMPLE_JSON))
def test_main(mock_post):
    mock_post.return_value.json.return_value = {"access_token": MOCK_ACCESS_TOKEN}
    mock_post.return_value.raise_for_status = lambda: None

    test_args = ["script.py", "--input_json", "test.json", "--env", "nonprod", "--rule_type", "validation", "--field_name", "name"]
    with patch("sys.argv", test_args):
        main()

    mock_post.assert_called()
