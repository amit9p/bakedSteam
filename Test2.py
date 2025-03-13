

import pytest
from unittest.mock import patch
from ecbr_calculations.edq.edq_rule_engine import main  # Import your function

def test_main_missing_keys():
    """Test main() when required keys 'rules' and 'job_id' are missing."""

    with patch("builtins.print") as mock_print:
        with patch("ecbr_calculations.edq.edq_rule_engine.parse_arguments") as mock_args:
            mock_args.return_value.rule_type = "validation"
            mock_args.return_value.field_name = ["name"]

            # Mock load_json_input to return an empty dictionary (no 'rules' or 'job_id')
            with patch("ecbr_calculations.edq.edq_rule_engine.load_json_input", return_value={}):
                main()

        # Assert that the expected message was printed
        mock_print.assert_any_call("Missing required input data: rules, job_id")
