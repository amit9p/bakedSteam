
def test_create_rules():
    with requests_mock.Mocker() as m:
        fake_endpoint = "https://api-it.cloud.capitalone.com/internal-operations/data-management/data-quality-configuration/job-configuration-rules/bulk-create"
        m.post(fake_endpoint, json={
            "successfulRuleList": [{"ruleId": "100", "ruleName": "TestRule1"}],
            "failedRuleList": [{"ruleName": "FailedRule", "errorCode": "ERR123"}]
        }, status_code=200)

        job_id = "job123"
        rule_list = [{"ruleName": "TestRule1"}, {"ruleName": "FailedRule"}]
        headers = {"Authorization": "Bearer test"}
        base_url = "https://api-it.cloud.capitalone.com"

        create_rules(job_id, rule_list, headers, base_url)


def test_update_rule():
    with requests_mock.Mocker() as m:
        fake_endpoint = "https://api-it.cloud.capitalone.com/internal-operations/data-management/data-quality-configuration/job-configuration-rules/001"
        m.patch(fake_endpoint, json={"ruleId": "001", "ruleName": "RuleA", "ruleVersion": "v1"}, status_code=200)

        rule_list = [{"ruleName": "RuleA"}]
        headers = {"Authorization": "Bearer test"}
        base_url = "https://api-it.cloud.capitalone.com"

        update_rule("001", rule_list, headers, base_url)


def test_delete_rule():
    with requests_mock.Mocker() as m:
        fake_endpoint = "https://api-it.cloud.capitalone.com/internal-operations/data-management/data-quality-configuration/job-configuration-rules/001"
        m.delete(fake_endpoint, status_code=200)

        headers = {"Authorization": "Bearer test"}
        base_url = "https://api-it.cloud.capitalone.com"

        delete_rule("001", headers, base_url)
