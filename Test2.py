
import pytest
from unittest.mock import patch
from ecbr_calculations.edq.edq_rule_engine import main  # Import your function

def test_main_missing_keys():
    """Test main() when required keys 'rules' and 'job_id' are missing."""

    with patch("builtins.print") as mock_print:
        with patch("ecbr_calculations.edq.edq_rule_engine.parse_arguments") as mock_args:
            mock_args.return_value.rule_type = "validation"
            mock_args.return_value.field_name = ["name"]

            # Mock load_json_input to return an empty dictionary (simulating missing 'rules' and 'job_id')
            with patch("ecbr_calculations.edq.edq_rule_engine.load_json_input", return_value={}):
                main()

        # Debug: Print what was actually captured
        print("\nCaptured mock print calls:")
        for call in mock_print.call_args_list:
            print(call)

        # Assert expected print statement is in the captured calls
        expected_message = "Missing required input data: rules, job_id"
        mock_print.assert_any_call(expected_message)
