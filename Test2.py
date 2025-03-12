
import pytest
from edq_rule_engine import parse_arguments
from unittest.mock import patch

def test_parse_arguments():
    test_args = [
        "script.py",
        "--env", "nonprod",
        "--rule_type", "validation",
        "--field_name", "name", "age"
    ]
    
    with patch("sys.argv", test_args):
        args = parse_arguments()
    
    assert args.env == "nonprod"
    assert args.rule_type == "validation"
    assert args.field_name == ["name", "age"]


from unittest.mock import patch, mock_open
import pytest
import requests
from edq_rule_engine import main

SAMPLE_JSON = '{"job_id": "123", "rules": {"validation": [{"ruleName": "Rule1"}]}}'
MOCK_ACCESS_TOKEN = "mocked_access_token"

@patch("requests.post")
@patch("builtins.open", mock_open(read_data=SAMPLE_JSON))  # Mock file reading
def test_main(mock_post):
    mock_post.return_value.json.return_value = {"access_token": MOCK_ACCESS_TOKEN}
    mock_post.return_value.raise_for_status = lambda: None  # ✅ Mock successful API call

    test_args = [
        "script.py",
        "--env", "nonprod",
        "--rule_type", "validation",
        "--field_name", "name"
    ]

    with patch("sys.argv", test_args):
        main()

    mock_post.assert_called()
