
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
