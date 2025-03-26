

import boto3

def list_s3_objects(
    access_key: str,
    secret_key: str,
    region: str,
    bucket_name: str,
    prefix: str
):
    """
    Lists all objects under a specified S3 bucket folder.

    :param access_key: AWS access key ID
    :param secret_key: AWS secret access key
    :param region: AWS region (e.g., 'us-east-1')
    :param bucket_name: Name of the S3 bucket
    :param prefix: Folder path (e.g. 'myfolder/') 
                   or '' for entire bucket
    """

    # Create a low-level service client by name using the given credentials
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )

    # Use a paginator to handle large listings automatically
    paginator = s3_client.get_paginator("list_objects_v2")

    # Paginate through all objects that start with the prefix
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        # Each page can contain up to 1000 keys
        contents = page.get("Contents", [])
        for obj in contents:
            # Print the object key (path) in the bucket
            print(obj["Key"])

if __name__ == "__main__":
    # Example usage
    list_s3_objects(
        access_key="<ACCESS_KEY>",
        secret_key="<SECRET_KEY>",
        region="<REGION>",           # e.g., 'us-east-1'
        bucket_name="<BUCKET_NAME>",
        prefix="myfolder/"           # or '' for entire bucket
    )





#################
import pytest
import requests
import requests_mock
import sys
from unittest.mock import patch, MagicMock

# Adjust imports to match your actual module paths:
from edq_rule_engine import create_rules, main

################################################################################
# Test: create_rules - success scenario (no dataFailingRules)
################################################################################
def test_create_rules_success():
    """
    Ensures create_rules() returns the expected response
    when the API call is successful and no dataFailingRules are present.
    """
    with requests_mock.Mocker() as m:
        # Mock successful response from the bulk-rules endpoint
        mock_resp = {
            "successfulRules": [
                {"ruleType": "rule1", "ruleName": "TestRule"}
            ],
            "ruleName": "RuleName",
            "errorCode": "ERR_123"
            # No dataFailingRules key here
        }
        m.post("https://some-base-url/data-management/rules-bulk",
               json=mock_resp, status_code=200)

        headers = {"Authorization": "Bearer test_token"}
        base_url = "https://some-base-url"

        # Call create_rules
        result = create_rules(
            rule_id="some_id",
            rule_list=["rule1"],
            dataFailingRules=[],    # pass an empty list or appropriate data
            headers=headers,
            base_url=base_url
        )

        # Verify the function returns the JSON or otherwise handles success
        assert result == mock_resp, "create_rules() should return the API response on success."


################################################################################
# Test: create_rules - dataFailingRules scenario
################################################################################
def test_create_rules_with_dataFailingRules():
    """
    If the API response contains 'dataFailingRules',
    ensure the code handles or raises an exception as intended.
    """
    with requests_mock.Mocker() as m:
        # Mock response that includes dataFailingRules
        mock_resp = {
            "successfulRules": [
                {"ruleType": "rule1", "ruleName": "TestRule"}
            ],
            "ruleName": "RuleName",
            "errorCode": "ERR_123",
            "dataFailingRules": [
                {"ruleName": "FailingRule", "reason": "Invalid config"}
            ]
        }
        m.post("https://some-base-url/data-management/rules-bulk",
               json=mock_resp, status_code=200)

        headers = {"Authorization": "Bearer test_token"}
        base_url = "https://some-base-url"

        # Depending on how your code behaves when dataFailingRules is present:
        #  - It might raise an exception
        #  - It might return a specific value
        #  - It might log a warning

        # Example: If your code raises an Exception:
        with pytest.raises(Exception, match="dataFailingRules found"):
            create_rules(
                rule_id="some_id",
                rule_list=["rule1"],
                dataFailingRules=[],
                headers=headers,
                base_url=base_url
            )


################################################################################
# Test: main() - verifying end-to-end flow
################################################################################
def test_main(monkeypatch):
    """
    Test the main() function by simulating command-line args and mocking
    external calls: parse_arguments, load_json_input, get_access_token, etc.
    """

    # 1) Mock sys.argv if your code uses argparse
    fake_argv = [
        "edq_rule_engine.py",
        "--env", "nonprod",
        "--rule_type", "non_suppressed",
        "--field_name", "first_name",
        "ssn.json"
    ]
    monkeypatch.setattr(sys, "argv", fake_argv)

    # 2) Mock parse_arguments() to return expected args
    mock_args = MagicMock()
    mock_args.env = "nonprod"
    mock_args.rule_type = "non_suppressed"
    mock_args.field_name = ["first_name"]
    mock_args.input_json = "ssn.json"

    # 3) Mock load_json_input() to return a dict with all required keys
    fake_json = {"rules": ["ruleA"], "job_id": "1234"}

    with patch("edq_rule_engine.parse_arguments", return_value=mock_args):
        with patch("edq_rule_engine.load_json_input", return_value=fake_json):
            # 4) Mock get_access_token so no real HTTP call is made
            with patch("edq_rule_engine.get_access_token", return_value="test_token"):
                # 5) Mock create_rules to ensure it doesn't actually call requests
                with patch("edq_rule_engine.create_rules", return_value={"status": "OK"}) as mock_create:
                    # Finally call main()
                    main()

                    # Assert create_rules was called with correct arguments
                    mock_create.assert_called_once()
                    # e.g. you can check the first call's arguments if needed:
                    # mock_create.call_args == call("some_id", [...], [...], {"Authorization":"Bearer test_token"}, "https://...")

    # You can add an assertion to check printed output using capsys, if your main() prints anything:
    # captured = capsys.readouterr()
    # assert "Finished Rules Creation" in captured.out
