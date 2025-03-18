
# test_edq_rule_engine.py

import pytest
import json
import sys
from unittest.mock import patch, MagicMock, mock_open
from edq_rule_engine import (
    get_access_token,
    build_rule_list,
    create_bulk_rules,
    main
)

@pytest.fixture
def mock_requests_post():
    """
    A pytest fixture to mock requests.post using unittest.mock.patch.
    We'll yield the mock so tests can configure responses as needed.
    """
    with patch("edq_rule_engine.requests.post") as mock_post:
        yield mock_post

# ------------------------
# Tests for get_access_token
# ------------------------

def test_get_access_token_success(mock_requests_post):
    # Mock a successful token response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"access_token": "fake-token"}
    mock_requests_post.return_value = mock_response

    token = get_access_token("my_client_id", "my_client_secret", "dev")
    assert token == "fake-token"
    mock_requests_post.assert_called_once()

def test_get_access_token_failure_status_code(mock_requests_post):
    # Mock a failure token response (non-200)
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "Bad Request"
    mock_requests_post.return_value = mock_response

    with pytest.raises(RuntimeError) as exc:
        get_access_token("my_client_id", "my_client_secret", "dev")

    assert "Token request failed with status code 400" in str(exc.value)
    mock_requests_post.assert_called_once()

def test_get_access_token_requests_exception(mock_requests_post):
    # Mock a requests exception
    mock_requests_post.side_effect = Exception("Connection error")

    with pytest.raises(RuntimeError) as exc:
        get_access_token("my_client_id", "my_client_secret", "dev")

    assert "Failed to get token: Connection error" in str(exc.value)

def test_get_access_token_unknown_env(mock_requests_post):
    # Should fail immediately because env not found
    with pytest.raises(ValueError) as exc:
        get_access_token("my_client_id", "my_client_secret", "invalid_env")

    assert "Unknown environment: invalid_env" in str(exc.value)
    mock_requests_post.assert_not_called()

# ------------------------
# Tests for build_rule_list
# ------------------------

def test_build_rule_list_success(mock_requests_post):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"result": "success"}
    mock_requests_post.return_value = mock_response

    headers = {"Authorization": "Bearer test", "Content-Type": "application/json"}
    result = build_rule_list([{"ruleName": "testRule"}], headers, "http://fake-base-url")

    assert result == {"result": "success"}
    mock_requests_post.assert_called_once()

def test_build_rule_list_failure(mock_requests_post):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Server Error"
    mock_requests_post.return_value = mock_response

    headers = {"Authorization": "Bearer test", "Content-Type": "application/json"}

    with pytest.raises(RuntimeError) as exc:
        build_rule_list([{"ruleName": "testRule"}], headers, "http://fake-base-url")

    assert "Failed to build rule list: Server Error" in str(exc.value)
    mock_requests_post.assert_called_once()

# ------------------------
# Tests for create_bulk_rules
# ------------------------

def test_create_bulk_rules_success(mock_requests_post):
    # Suppose we have two rules to create
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"ruleCreated": True}
    mock_requests_post.return_value = mock_response

    headers = {"Authorization": "Bearer test", "Content-Type": "application/json"}
    rules = [{"ruleName": "rule1"}, {"ruleName": "rule2"}]

    results = create_bulk_rules(rules, headers, "http://fake-base-url")
    # Expect two calls, one per rule
    assert len(results) == 2
    assert results[0] == {"ruleCreated": True}
    assert results[1] == {"ruleCreated": True}
    assert mock_requests_post.call_count == 2

def test_create_bulk_rules_failure(mock_requests_post):
    # Simulate a failure on the second rule
    success_response = MagicMock()
    success_response.status_code = 200
    success_response.json.return_value = {"ruleCreated": True}

    failure_response = MagicMock()
    failure_response.status_code = 400
    failure_response.text = "Bad Request"

    mock_requests_post.side_effect = [success_response, failure_response]

    headers = {"Authorization": "Bearer test", "Content-Type": "application/json"}
    rules = [{"ruleName": "rule1"}, {"ruleName": "rule2"}]

    with pytest.raises(RuntimeError) as exc:
        create_bulk_rules(rules, headers, "http://fake-base-url")

    assert "Failed to create rule: Bad Request" in str(exc.value)
    assert mock_requests_post.call_count == 2

# ------------------------
# Tests for main()
# ------------------------

@pytest.mark.parametrize("env", ["dev", "qa", "prod"])
def test_main_success(env, mock_requests_post, monkeypatch):
    """
    Test main() with a valid environment, mocking sys.argv to simulate
    command-line arguments and mocking file IO for the rule file.
    """
    # 1) Mock token response
    token_response = MagicMock()
    token_response.status_code = 200
    token_response.json.return_value = {"access_token": "test-token"}

    # 2) Mock build_rule_list response
    build_response = MagicMock()
    build_response.status_code = 200
    build_response.json.return_value = {"build": "ok"}

    # 3) Mock create_bulk_rules response
    bulk_response = MagicMock()
    bulk_response.status_code = 200
    bulk_response.json.return_value = {"bulk": "ok"}

    # We'll get 1 token request, 1 build rule request, 
    # and then possibly multiple bulk rule requests
    mock_requests_post.side_effect = [token_response, build_response, bulk_response, bulk_response]

    # Simulate a JSON file with rule_list and bulk_rules
    fake_rules_json = json.dumps({
        "rule_list": [{"ruleName": "someRule"}],
        "bulk_rules": [{"ruleName": "bulk1"}, {"ruleName": "bulk2"}]
    })

    # Mock open() so that reading the file returns our fake JSON
    m = mock_open(read_data=fake_rules_json)
    with patch("builtins.open", m):
        # Monkeypatch command-line arguments
        test_args = [
            "prog",
            "--env", env,
            "--client_id", "test_client_id",
            "--client_secret", "test_client_secret",
            "--rule_file", "fake_rules.json"
        ]
        monkeypatch.setattr(sys, "argv", test_args)

        # Capture output
        with patch("sys.stdout", new_callable=lambda: MagicMock()) as mock_stdout:
            main()

    # Verify we printed success
    assert mock_stdout.write.call_count > 0
    printed_text = "".join(call_args[0] for call_args, _ in mock_stdout.write.call_args_list)
    assert "Rules created successfully!" in printed_text

    # We expect 1 token call, 1 build_rule_list call, 2 create_bulk_rules calls
    assert mock_requests_post.call_count == 1 + 1 + 2

def test_main_unknown_env(mock_requests_post, monkeypatch):
    # If we pass an unknown environment, we should fail before requests.post is called
    test_args = [
        "prog",
        "--env", "staging",  # Not in env_urls
        "--client_id", "test_client_id",
        "--client_secret", "test_client_secret",
        "--rule_file", "fake_rules.json"
    ]
    monkeypatch.setattr(sys, "argv", test_args)

    # We don't need to mock open in this scenario if the code tries to parse the env first
    with pytest.raises(ValueError) as exc:
        main()
    assert "Unknown environment" in str(exc.value)
    mock_requests_post.assert_not_called()

def test_main_request_failure(mock_requests_post, monkeypatch):
    # Suppose the token request fails
    mock_requests_post.side_effect = Exception("Connection refused")

    # Provide normal arguments
    test_args = [
        "prog",
        "--env", "dev",
        "--client_id", "test_client_id",
        "--client_secret", "test_client_secret",
        "--rule_file", "fake_rules.json"
    ]
    monkeypatch.setattr(sys, "argv", test_args)

    # We need to mock open, because main() will try to open the rule_file
    m = mock_open(read_data=json.dumps({"rule_list": []}))
    with patch("builtins.open", m):
        with pytest.raises(RuntimeError) as exc:
            main()
    assert "Failed to get token: Connection refused" in str(exc.value)
    mock_requests_post.assert_called_once()
