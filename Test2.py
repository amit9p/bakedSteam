

from unittest.mock import patch, MagicMock, mock_open
import json
import pytest
import sys
from pathlib import Path
import requests_mock

from ecbr_card_self_service.edq.ecbr_calculations.scripts.edq_rule_engine import (
    parse_arguments,
    load_json_input,
    get_access_token,
    create_rules,
    update_rule,
    delete_rule,
    main,
)

def test_parse_arguments(monkeypatch):
    test_args = [
        "script_name",
        "--env", "nonprod",
        "--rule_type", "myRuleType",
        "--operation_type", "create",
        "--field_name", "field1", "field2"
    ]
    monkeypatch.setattr(sys, "argv", test_args)
    args = parse_arguments()
    assert args.env == "nonprod"
    assert args.rule_type == "myRuleType"
    assert args.operation_type == "create"
    assert args.field_name == ["field1", "field2"]

def test_load_json_input():
    fake_json = {
        "job_id": "12345",
        "rules": [{"ruleName": "testRule1"}, {"ruleName": "testRule2"}]
    }
    with patch("builtins.open", mock_open(read_data=json.dumps(fake_json))):
        data = load_json_input("created_path.json")
        assert data["job_id"] == "12345"
        assert len(data["rules"]) == 2

@pytest.mark.parametrize("env,url_part", [("nonprod", "api-it.cloud.capitalone.com"), ("prod", "api.cloud.capitalone.com")])
def test_get_access_token(env, url_part):
    with requests_mock.Mocker() as m:
        token_url = f"https://{url_part}/oauth2/token"
        m.post(token_url, json={"access_token": "fake_token_value"}, status_code=200)
        token_response = get_access_token("fake_id", "fake_secret", env)
        assert token_response == "fake_token_value"

def test_create_rules():
    with requests_mock.Mocker() as m:
        fake_endpoint = "https://api-it.cloud.capitalone.com/internal-operations/data-management/data-quality-configuration/job-configuration-rules/bulk-create"
        m.post(fake_endpoint, json={
            "successfulRuleList": [{"ruleId": "100", "ruleName": "TestRule1"}],
            "failedRuleList": [{"ruleName": "FailedRule", "errorCode": "ERR123"}]
        }, status_code=200)
        create_rules("job123", [{"ruleName": "TestRule1"}, {"ruleName": "FailedRule"}], {"Authorization": "Bearer test"}, "https://api-it.cloud.capitalone.com")

def test_update_rule():
    with requests_mock.Mocker() as m:
        fake_endpoint = "https://api-it.cloud.capitalone.com/internal-operations/data-management/data-quality-configuration/job-configuration-rules/001"
        m.patch(fake_endpoint, json={"ruleId": "001", "ruleName": "RuleA", "ruleVersion": "v1"}, status_code=200)
        update_rule("001", [{"ruleName": "RuleA"}], {"Authorization": "Bearer test"}, "https://api-it.cloud.capitalone.com")

def test_delete_rule():
    with requests_mock.Mocker() as m:
        fake_endpoint = "https://api-it.cloud.capitalone.com/internal-operations/data-management/data-quality-configuration/job-configuration-rules/001"
        m.delete(fake_endpoint, status_code=200)
        delete_rule("001", {"Authorization": "Bearer test"}, "https://api-it.cloud.capitalone.com")

def test_main(monkeypatch):
    test_args = [
        "script_name",
        "--env", "nonprod",
        "--rule_type", "myRuleType",
        "--operation_type", "create",
        "--field_name", "field1"
    ]
    monkeypatch.setattr(sys, "argv", test_args)

    fake_json = {
        "job_id": "fake_job",
        "rules": {
            "myRuleType": [{"ruleName": "RuleA"}, {"ruleName": "RuleB"}]
        }
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(fake_json))):
        with requests_mock.Mocker() as m:
            m.post("https://api-it.cloud.capitalone.com/oauth2/token", json={"access_token": "fake_token_for_main"}, status_code=200)

            m.post("https://api-it.cloud.capitalone.com/internal-operations/data-management/data-quality-configuration/job-configuration-rules/bulk-create",
                   json={"successfulRuleList": [{"ruleId": "001", "ruleName": "RuleA"}], "failedRuleList": []}, status_code=200)

            try:
                main()
            except Exception as e:
                pytest.fail(f"main() raised an unexpected exception: {e}")
