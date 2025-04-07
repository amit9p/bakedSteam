
^(?:(Apt|Apartment|Suite|Ste|Unit|Rm|Room|Bldg|Building|FL|Floor)\s*#?\s*\w+.*)?$

    ^...$ → Ensures it matches the entire line

(?:...) → Non-capturing group

Apt|Suite|Unit|... → Acceptable address types

\s*#?\s* → Optional whitespace and optional “#”

\w+.* → At least one character or number and rest of the line (e.g., 4B, 5, Room 202)

? → The whole thing is optional since some people leave Address Line 2 blank



from unittest.mock import patch
import requests

def test_create_rules():
    with patch("ecbr_card_self_service.edq.ecbr_calculations.scripts.edq_rule_engine.cert_path", None):
        with patch("requests.post") as mock_post:
            mock_response = requests.Response()
            mock_response.status_code = 200
            mock_response._content = b'''
            {
                "successfulRuleList": [{"ruleId": "001", "ruleName": "TestRule"}],
                "failedRuleList": []
            }
            '''
            mock_post.return_value = mock_response

            job_id = "job123"
            rule_list = [{"ruleName": "TestRule"}]
            headers = {"Authorization": "Bearer test"}
            base_url = "https://api-it.cloud.capitalone.com"

            create_rules(job_id, rule_list, headers, base_url)

            mock_post.assert_called_once()
            assert "bulk-create" in mock_post.call_args[0][0]




def test_update_rule():
    with patch("ecbr_card_self_service.edq.ecbr_calculations.scripts.edq_rule_engine.cert_path", None):
        with patch("requests.patch") as mock_patch:
            mock_response = requests.Response()
            mock_response.status_code = 200
            mock_response._content = b'''
            {
                "ruleId": "001",
                "ruleName": "UpdatedRule",
                "ruleVersion": "v2"
            }
            '''
            mock_patch.return_value = mock_response

            rule_list = [{"ruleName": "UpdatedRule"}]
            headers = {"Authorization": "Bearer test"}
            base_url = "https://api-it.cloud.capitalone.com"

            update_rule("001", rule_list, headers, base_url)

            mock_patch.assert_called_once()
            assert "job-configuration-rules/001" in mock_patch.call_args[0][0]








from unittest.mock import patch
import requests

def test_delete_rule():
    with patch("ecbr_card_self_service.edq.ecbr_calculations.scripts.edq_rule_engine.cert_path", None):
        with patch("requests.delete") as mock_delete:
            mock_response = requests.Response()
            mock_response.status_code = 200
            mock_delete.return_value = mock_response

            headers = {"Authorization": "Bearer test"}
            base_url = "https://api-it.cloud.capitalone.com"

            delete_rule("001", headers, base_url)

            mock_delete.assert_called_once()
            called_url = mock_delete.call_args[0][0]
            assert "job-configuration-rules/001" in called_url
