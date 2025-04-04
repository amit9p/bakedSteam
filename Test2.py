

@patch.dict(os.environ, {
    "HTTP_PROXY": "", "http_proxy": "",
    "HTTPS_PROXY": "", "https_proxy": ""
})
def test_delete_rule():
    with patch("ecbr_card_self_service.edq.ecbr_calculations.scripts.edq_rule_engine.cert_path", None):
        with requests_mock.Mocker() as m:
            url = "https://api-it.cloud.capitalone.com/internal-operations/data-management/data-quality-configuration/job-configuration-rules/001"
            m.delete(url, status_code=200)

            headers = {"Authorization": "Bearer test"}
            base_url = "https://api-it.cloud.capitalone.com"

            try:
                delete_rule("001", headers, base_url)
            except Exception as e:
                # debug any actual request mismatch
                print("\nMocked URLs:")
                for req in m.request_history:
                    print("  >>", req.method, req.url)
                raise e
