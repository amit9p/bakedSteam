

def test_create_rules():
    import requests_mock
    import pytest
    from edq_rule_engine import create_rules

    with requests_mock.Mocker() as m:
        # EXACT endpoint your code calls:
        #  => "https://api-it.cloud.capitalone.com/internal-operations/data-management/data-quality-configuration/job-configuration-rules/bulk.create"
        fake_endpoint = (
            "https://api-it.cloud.capitalone.com/"
            "internal-operations/data-management/"
            "data-quality-configuration/job-configuration-rules/bulk.create"
        )

        # Mock response
        m.post(fake_endpoint, json={
            "successfulRuleList": [{"ruleId": "100", "ruleName": "TestRule1"}],
            "failedRuleList": [{"ruleName": "FailedRule", "errorCode": "ERR123"}]
        }, status_code=200)

        # Match the exact values your real code passes in:
        job_id = "job123"
        rule_list = [
            {"ruleName": "TestRule1"},
            {"ruleName": "FailedRule"}
        ]
        headers  = {"Authorization": "Bearer test_token"}
        # If your code uses base_url + "/internal-operations/...", then
        # base_url should NOT have a trailing slash. Adjust to match:
        base_url = "https://api-it.cloud.capitalone.com"

        # Run the actual function
        try:
            create_rules(job_id, rule_list, headers, base_url)
        except Exception as exc:
            pytest.fail(f"create_rules raised an unexpected exception: {exc}")
