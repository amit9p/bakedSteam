
def test_create_rules():
    import requests_mock
    import pytest
    from edq_rule_engine import create_rules

    with requests_mock.Mocker() as m:
        fake_endpoint = (
            "https://api-it.cloud.capitalone.com/"
            "internal-operations/data-management/"
            "data-quality-configuration/job-configuration-rules/bulk-create"
        )
        # If your code doesn’t include the trailing slash, omit it in the test, etc.

        # Mock response
        m.post(
            fake_endpoint,
            json={
                "successfulRuleList": [
                    {"ruleId": "100", "ruleName": "TestRule1"}
                ],
                "failedRuleList": [
                    {"ruleName": "FailedRule", "errorCode": "ERR123"}
                ]
            },
            status_code=200
        )

        # Match the exact values your real code passes:
        job_id = "job123"
        rule_list = [
            {"ruleName": "TestRule1"},
            {"ruleName": "FailedRule"}
        ]
        headers  = {"Authorization": "Bearer test_token"}
        # If your code does base_url + "/internal-operations/...", then base_url
        # might be "https://api-it.cloud.capitalone.com" (no trailing slash).
        base_url = "https://api-it.cloud.capitalone.com"

        try:
            create_rules(job_id, rule_list, headers, base_url)
        except Exception as exc:
            pytest.fail(f"create_rules raised an unexpected Exception: {exc}")
