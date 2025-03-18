
import json
import pytest
import sys
from unittest.mock import patch, mock_open
from pathlib import Path

# If you use requests_mock, install via pip and import:
import requests_mock

# Import functions/classes from your code under test
# Adjust these imports to match your project structure
from edq_rule_engine import (
    parse_arguments,
    load_json_input,
    get_access_token,
    create_rules,
    main
)

#
# 1. Test parse_arguments
#
def test_parse_arguments(monkeypatch):
    """
    Checks whether parse_arguments correctly parses the command-line flags.
    """
    # Fake command line:
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


#
# 2. Test load_json_input
#
def test_load_json_input():
    """
    Ensures load_json_input reads and returns the JSON content from the given file path.
    """
    fake_json = {
        "job_id": "12345",
        "rules": [
            {"ruleName": "testRule1"},
            {"ruleName": "testRule2"}
        ]
    }

    # Mock open(...) to return our fake JSON
    with patch("builtins.open", mock_open(read_data=json.dumps(fake_json))):
        data = load_json_input("dummy_path.json")

    assert data["job_id"] == "12345"
    assert len(data["rules"]) == 2


#
# 3. Test get_access_token
#
@pytest.mark.parametrize("env,url_part", [
    ("nonprod", "api-it.cloud.capitalone.com"),
    ("prod",    "api.cloud.capitalone.com")
])
def test_get_access_token(env, url_part):
    """
    Mocks the OAuth2 call using requests_mock and verifies we parse token JSON.
    """
    # If using requests_mock fixture, the function signature would be
    #   def test_get_access_token(env, url_part, requests_mock):
    # For demonstration, we show manual usage of requests_mock here:
    with requests_mock.Mocker() as m:
        # Construct the expected endpoint for your environment
        token_url = f"https://{url_part}/oauth2/token"

        # Mock the POST response
        m.post(token_url, json={"access_token": "fake_token_value"}, status_code=200)

        token_response = get_access_token(
            client_id="fake_id",
            client_secret="fake_secret",
            env=env
        )
        assert token_response["access_token"] == "fake_token_value"


#
# 4. Test create_rules
#
def test_create_rules():
    """
    Verifies create_rules correctly POSTs and processes success/failure lists.
    """
    # We’ll mock out the network call with requests_mock again:
    with requests_mock.Mocker() as m:
        # Suppose your code calls something like:
        #   base_url + "internal-operations/data-management/data-quality-configuration/..."
        # So we'll mock that endpoint:
        fake_endpoint = (
            "https://api-it.cloud.capitalone.com/"  # or whichever environment
            "internal-operations/data-management/"
            "data-quality-configuration/job-configuration-rules/bulk"
        )

        # Fake response from the rule creation call
        mock_response_body = {
            "successfulRuleList": [
                {"ruleId": "100", "ruleName": "TestRule1"}
            ],
            "failedRuleList": [
                {"ruleName": "FailedRule", "errorCode": "ERR123"}
            ]
        }
        m.post(fake_endpoint, json=mock_response_body, status_code=200)

        # Minimal arguments
        job_id    = "job123"
        rule_list = [{"ruleName": "TestRule1"}, {"ruleName": "FailedRule"}]
        headers   = {"Authorization": "Bearer test_token"}
        base_url  = "https://api-it.cloud.capitalone.com/"  # or "https://api.cloud.capitalone.com/"

        # We just verify that it doesn't raise and hits the mock endpoint:
        try:
            create_rules(job_id, rule_list, headers, base_url)
        except Exception as exc:
            pytest.fail(f"create_rules raised an unexpected Exception: {exc}")

        # Optional: check request body, etc.
        history = m.request_history
        assert len(history) == 1
        posted_json = json.loads(history[0].text)
        assert posted_json["ruleList"] == rule_list
        assert posted_json["jobId"] == job_id


#
# 5. Test main
#
def test_main(monkeypatch):
    """
    Integration-style test that patches:
      - sys.argv to simulate command-line calls,
      - load_json_input so we don't read real files,
      - requests for token retrieval & rule creation,
      - etc.

    Then calls main() and ensures it runs through w/o error.
    """
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

    # 1) Mock out load_json_input so we don't need real files
    with patch("builtins.open", mock_open(read_data=json.dumps(fake_json))):
        # 2) Mock requests inside get_access_token and create_rules
        with requests_mock.Mocker() as m:
            # Mock the token endpoint
            token_url = "https://api-it.cloud.capitalone.com/oauth2/token"
            m.post(token_url, json={"access_token": "fake_token_for_main"}, status_code=200)

            # Mock the create_rules endpoint
            fake_create_endpoint = (
                "https://api-it.cloud.capitalone.com/"
                "internal-operations/data-management/"
                "data-quality-configuration/job-configuration-rules/bulk"
            )
            create_resp = {
                "successfulRuleList": [{"ruleId": "001", "ruleName": "RuleA"}],
                "failedRuleList": []
            }
            m.post(fake_create_endpoint, json=create_resp, status_code=200)

            # Finally, call main()
            #   If everything is correct, we expect no exception:
            try:
                main()
            except Exception as e:
                pytest.fail(f"main() raised an unexpected exception: {e}")

    # Optionally assert certain prints or certain calls happened, etc.
    # For example, you could check m.request_history or capture stdout with capsys
