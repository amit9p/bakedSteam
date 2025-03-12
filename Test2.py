
import pytest
import json
import requests
from unittest.mock import patch, MagicMock
from edq_rule_engine import get_access_token, create_rules, main


# Mock Data
MOCK_ENV_URLS = {
    "nonprod": "https://api-nonprod.cloud.example.com",
    "prod": "https://api.cloud.example.com",
}
MOCK_ACCESS_TOKEN = "mocked_token"
MOCK_JOB_ID = "1234"
MOCK_RULES = [{"ruleId": "001", "ruleName": "Rule_A"}]


@pytest.fixture
def mock_env(monkeypatch):
    """Mock environment URLs."""
    monkeypatch.setattr("edq_rule_engine.env_urls", MOCK_ENV_URLS)


@pytest.fixture
def mock_requests_post():
    """Mock requests.post for API calls."""
    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": MOCK_ACCESS_TOKEN}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response
        yield mock_post


def test_get_access_token_success(mock_env, mock_requests_post):
    """Test get_access_token with a valid environment."""
    token = get_access_token("client_id", "client_secret", "nonprod")
    assert token == MOCK_ACCESS_TOKEN


def test_get_access_token_invalid_env(mock_env):
    """Test get_access_token with an invalid environment."""
    with pytest.raises(Exception, match="Unknown environment"):
        get_access_token("client_id", "client_secret", "invalid_env")


def test_get_access_token_request_failure(mock_env):
    """Test get_access_token when API call fails."""
    with patch("requests.post", side_effect=requests.exceptions.RequestException):
        with pytest.raises(RuntimeError, match="Failed to create access token"):
            get_access_token("client_id", "client_secret", "nonprod")


def test_create_rules_success(mock_requests_post):
    """Test successful rule creation."""
    headers = {"Authorization": f"Bearer {MOCK_ACCESS_TOKEN}"}
    base_url = MOCK_ENV_URLS["nonprod"]
    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.json.return_value = {"successfulRulesList": MOCK_RULES}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        create_rules(MOCK_JOB_ID, MOCK_RULES, headers, base_url)

        mock_post.assert_called_once()


def test_create_rules_failure(mock_requests_post):
    """Test rule creation failure scenario."""
    headers = {"Authorization": f"Bearer {MOCK_ACCESS_TOKEN}"}
    base_url = MOCK_ENV_URLS["nonprod"]
    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.json.return_value = {"failedRulesList": MOCK_RULES}
        mock_response.raise_for_status = MagicMock()
        mock_post.return_value = mock_response

        create_rules(MOCK_JOB_ID, MOCK_RULES, headers, base_url)

        mock_post.assert_called_once()


def test_create_rules_request_exception():
    """Test exception handling in create_rules."""
    headers = {"Authorization": f"Bearer {MOCK_ACCESS_TOKEN}"}
    base_url = MOCK_ENV_URLS["nonprod"]
    with patch("requests.post", side_effect=requests.exceptions.RequestException):
        with pytest.raises(Exception, match="An error occurred while creating the rules"):
            create_rules(MOCK_JOB_ID, MOCK_RULES, headers, base_url)


def test_main_invalid_env():
    """Test main function with invalid environment."""
    with patch("builtins.print") as mock_print:
        with patch("edq_rule_engine.parse_arguments") as mock_args:
            mock_args.return_value.env = "invalid_env"
            main()
            mock_print.assert_any_call("Unknown environment: invalid_env")


def test_main_missing_keys():
    """Test main function with missing input keys."""
    with patch("builtins.print") as mock_print:
        with patch("edq_rule_engine.parse_arguments") as mock_args:
            mock_args.return_value.rule_type = "validation"
            mock_args.return_value.field_name = ["name"]

            with patch("edq_rule_engine.load_json_input", return_value={}):
                main()
                mock_print.assert_any_call("Missing required input data: rules, job_id")


def test_main_invalid_rule_type():
    """Test main function with an invalid rule type."""
    with patch("builtins.print") as mock_print:
        with patch("edq_rule_engine.parse_arguments") as mock_args:
            mock_args.return_value.env = "nonprod"
            mock_args.return_value.rule_type = "invalid_type"
            mock_args.return_value.field_name = ["name"]

            with patch("edq_rule_engine.load_json_input", return_value={"rules": {}, "job_id": "123"}):
                main()
                mock_print.assert_any_call("Invalid rules provided, 'invalid_type' not found in input")


def test_main_success():
    """Test main function successfully executing."""
    with patch("builtins.print") as mock_print:
        with patch("edq_rule_engine.parse_arguments") as mock_args:
            mock_args.return_value.env = "nonprod"
            mock_args.return_value.rule_type = "validation"
            mock_args.return_value.field_name = ["name"]

            with patch("edq_rule_engine.load_json_input", return_value={"rules": {"validation": {}}, "job_id": "123"}):
                with patch("edq_rule_engine.get_access_token", return_value=MOCK_ACCESS_TOKEN):
                    with patch("edq_rule_engine.create_rules") as mock_create_rules:
                        main()
                        mock_create_rules.assert_called_once()
                        mock_print.assert_any_call("======= Finished Rules Creation for Dataset =======")
                        
