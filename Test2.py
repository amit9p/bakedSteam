
import pytest
from unittest.mock import patch
from ecbr_calculations.edq.edq_rule_engine import main  

def test_main_missing_keys_debug():
    """Debug: Print all captured outputs from main() when required keys are missing."""

    with patch("builtins.print") as mock_print:
        with patch("ecbr_calculations.edq.edq_rule_engine.parse_arguments") as mock_args:
            mock_args.return_value.rule_type = "validation"
            mock_args.return_value.field_name = ["name"]

            # Mock load_json_input to return an empty dictionary (simulating missing 'rules' and 'job_id')
            with patch("ecbr_calculations.edq.edq_rule_engine.load_json_input", return_value={}):
                try:
                    main()
                except SystemExit:
                    pass  # Catch sys.exit() if main() exits the process

        # Debugging: Print all mock calls to verify expected output
        print("\nCaptured mock print calls:")
        for call in mock_print.call_args_list:
            print(call)
