
import pytest
import requests_mock
from edq_rule_engine import create_rules

def test_create_rules():
    """
    Tests that create_rules() successfully POSTS to the correct endpoint,
    handles success/fail lists, and does not raise an Exception.
    """
    # 1. We'll use requests_mock to intercept the outbound POST.
    with requests_mock.Mocker() as m:
        # 2. This must match exactly the final URL in create_rules.
        #    If base_url ends with '/', then the endpoint is appended directly.
        fake_endpoint = (
            "https://api-it.cloud.capitalone.com/"
            "internal-operations/data-management/"
            "data-quality-configuration/job-configuration-rules/bulk-create"
        )

        # 3. Prepare a mock response that your code expects
        mock_resp = {
            "successfulRuleList": [
                {"ruleId": "100", "ruleName": "TestRule1"}
            ],
            "failedRuleList": [
                {"ruleName": "FailedRule", "errorCode": "ERR123"}
            ]
        }
        m.post(fake_endpoint, json=mock_resp, status_code=200)

        # 4. Inputs matching your code’s function signature
        job_id    = "job123"
        rule_list = [
            {"ruleName": "TestRule1"},
            {"ruleName": "FailedRule"}
        ]
        headers   = {"Authorization": "Bearer test_token"}
        base_url  = "https://api-it.cloud.capitalone.com/"  # must end with / if your code simply does base_url + endpoint

        # 5. Call create_rules and ensure no exception occurs
        try:
            create_rules(job_id, rule_list, headers, base_url)
        except Exception as exc:
            pytest.fail(f"create_rules raised an unexpected Exception: {exc}")

        # 6. (Optional) Inspect what was actually POSTed
        history = m.request_history
        assert len(history) == 1
        posted_request = history[0]
        # Confirm the JSON body
        assert posted_request.json() == {
            "rulesList": rule_list,
            "jobId": job_id
        }
        # Confirm headers if desired
        assert posted_request.headers.get("Authorization") == "Bearer test_token"
