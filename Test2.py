
import requests
import json

def process_failed_rules(base_url, result_api_url, job_id, datasetConfigurationId, oauthToken):
    url = base_url + result_api_url + "?jobId=" + job_id + "&datasetConfigurationId=" + datasetConfigurationId
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json;v=1",
        "Authorization": "Bearer " + str(oauthToken)
    }

    response = requests.get(url, headers=headers, verify=False)
    data = json.loads(response.text)

    field_account_dict = {}

    entry = data[0]
    rules = entry.get("ruleResults", [])

    for rule in rules:
        if rule.get("result") == "FAIL":
            field_name = rule.get("fieldName")
            failing_samples = rule.get("failingRuleSampleData", {})
            data_blocks = failing_samples.get("data", [])

            for data_list in data_blocks:
                for d in data_list:
                    if d.get("fieldName") == "account_id":
                        account_id = d.get("value")

                        if field_name not in field_account_dict:
                            field_account_dict[field_name] = []

                        field_account_dict[field_name].append(account_id)

    return field_account_dict


def main():
    base_url = "https://api.example.com/"
    result_api_url = "data-management/data-quality-results/rules-result"
    job_id = "your_job_id_here"
    datasetConfigurationId = "your_dataset_config_id_here"
    oauthToken = "your_oauth_token_here"

    result = process_failed_rules(base_url, result_api_url, job_id, datasetConfigurationId, oauthToken)
    print("Failed Rules Account IDs by Field:")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
