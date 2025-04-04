
from unittest.mock import patch
import os
import requests_mock

@patch.dict(os.environ, {
    "HTTP_PROXY": "", "http_proxy": "",
    "HTTPS_PROXY": "", "https_proxy": ""
})
def test_delete_rule():
    with patch("ecbr_card_self_service.edq.ecbr_calculations.scripts.edq_rule_engine.cert_path", None):
        with requests_mock.Mocker() as m:
            fake_endpoint = "https://api-it.cloud.capitalone.com/internal-operations/data-management/data-quality-configuration/job-configuration-rules/001"
            m.delete(fake_endpoint, status_code=200)

            headers = {"Authorization": "Bearer test"}
            base_url = "https://api-it.cloud.capitalone.com"

            delete_rule("001", headers, base_url)
