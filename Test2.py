

import json
import pytest
import sys
from unittest.mock import patch, mock_open
import requests_mock

from edq_rule_engine import (
    parse_arguments,
    load_json_input,
    get_access_token,
    create_rules,
    main
)

def test_parse_arguments(monkeypatch):
    test_args = [
        "script_name",
        "--env", "nonprod",
        "--rule_type", "myRuleType",
        "--field_name", "field1", "field2"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    args = parse_arguments()

    assert args.env == "nonprod"
    assert args.rule_type == "myRuleType"
    assert args.field_name == ["field1", "field2"]

def test_load_json_input():
    fake_json = {
        "job_id": "12345",
        "rules": [
            {"ruleName": "testRule1"},
            {"ruleName": "testRule2"}
        ]
    }
    with patch("builtins.open", mock_open(read_data=json.dumps(fake_json))):
        data = load_json_input("dummy_path.json")

    assert data["job_id"] == "12345"
    assert len(data["rules"]) == 2

@pytest.mark.parametrize("env,url_part", [
    ("nonprod", "api-it.cloud.capitalone.com"),
    ("prod",    "api.cloud.capitalone.com")
])
def test_get_access_token(env, url_part):
    """
    If your implementation returns just the string token,
    we must do 'assert token_response == "..."'
    instead of token_response["access_token"].
    """
    with requests_mock.Mocker() as m:
        # Match exactly what your code calls in get_access_token
        token_url = f"https://{url_part}/oauth2/token"
        m.post(token_url, json={"access_token": "fake_token_value"}, status_code=200)

        token_response = get_access_token(
            client_id="fake_id",
            client_secret="fake_secret",
            env=env
        )
        # If your code returns only the token string, do this:
        # e.g. return resp.json()["access_token"]
        assert token_response == "fake_token_value"

def test_create_rules():
    """
    Make sure the URL below matches precisely what your code calls.
    For instance, if the code calls:
        POST https://api-it.cloud.capitalone.com/internal-operations/data-management/...
    then we mock that exact address.
    """
    with requests_mock.Mocker() as m:
        fake_endpoint = (
            "https://api-it.cloud.capitalone.com/"
            "internal-operations/data-management/"
            "data-quality-configuration/job-configuration-rules/bulk"
        )
        # If your code omits the trailing slash, remove it here, etc.
        m.post(fake_endpoint, json={
            "successfulRuleList": [
                {"ruleId": "100", "ruleName": "TestRule1"}
            ],
            "failedRuleList": [
                {"ruleName": "FailedRule", "errorCode": "ERR123"}
            ]
        }, status_code=200)

        job_id    = "job123"
        rule_list = [{"ruleName": "TestRule1"}, {"ruleName": "FailedRule"}]
        headers   = {"Authorization": "Bearer test_token"}
        base_url  = "https://api-it.cloud.capitalone.com/"  # or exactly what your code expects

        try:
            create_rules(job_id, rule_list, headers, base_url)
        except Exception as exc:
            pytest.fail(f"create_rules raised an unexpected Exception: {exc}")

def test_main(monkeypatch):
    test_args = [
        "script_name",
        "--env", "nonprod",
        "--rule_type", "myRuleType",
        "--field_name", "myField"
    ]
    monkeypatch.setattr(sys, "argv", test_args)

    fake_json = {
        "job_id": "fake_job",
        "rules": {
            "myRuleType": [
                {"ruleName": "RuleA"},
                {"ruleName": "RuleB"}
            ]
        }
    }
    with patch("builtins.open", mock_open(read_data=json.dumps(fake_json))):
        with requests_mock.Mocker() as m:
            # Mock the token endpoint exactly
            token_url = "https://api-it.cloud.capitalone.com/oauth2/token"
            m.post(token_url, json={"access_token": "fake_token_for_main"}, status_code=200)

            # Mock the create_rules endpoint exactly
            fake_create_endpoint = (
                "https://api-it.cloud.capitalone.com/"
                "internal-operations/data-management/"
                "data-quality-configuration/job-configuration-rules/bulk"
            )
            m.post(fake_create_endpoint, json={
                "successfulRuleList": [{"ruleId": "001", "ruleName": "RuleA"}],
                "failedRuleList": []
            }, status_code=200)

            try:
                main()  # If it raises, we catch it
            except Exception as e:
                pytest.fail(f"main() raised an unexpected exception: {e}")
