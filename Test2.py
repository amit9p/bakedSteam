
from unittest.mock import patch

def test_main_missing_keys():
    """Test main function with missing required keys."""

    with patch("builtins.print") as mock_print:
        with patch("ecbr_calculations.edq.edq_rule_engine.parse_arguments") as mock_args:
            mock_args.return_value.rule_type = "validation"
            mock_args.return_value.field_name = ["name"]

            # Mock load_json_input to return an empty dictionary (missing 'rules' and 'job_id')
            with patch("ecbr_calculations.edq.edq_rule_engine.load_json_input", return_value={}):
                main()

        # Debug: Print what was actually captured in mock_print
        print("\nACTUAL PRINT CALLS:")
        for call in mock_print.call_args_list:
            print(call)

        # Verify that the missing keys message was printed
        mock_print.assert_any_call("Missing required input data: rules, job_id")
