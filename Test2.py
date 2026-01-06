optional_args["output_filetype"] = "ALL"

req_args["enable_detokenization"] = True
optional_args["output_filetype"] = "ALL"

common_utils_instance.get_args_for_job.return_value = (
    req_args,
    optional_args
)





mock_validate_detokenized_df.return_value = 0


mock_validate_detokenized_df.return_value = error_count

mock_metrics_instance.set_detokenization_error_count.assert_called_once_with(error_count)
mock_metrics_instance.write_metrics.assert_called_once()


self.assertEqual(error_count, 0)
common.logger.info.assert_any_call(
    "Detokenization validation passed: No ERROR strings found in formatted column"
)

common.logger.error.assert_not_called()
common.logger.warning.assert_not_called()


common.logger.info.assert_any_call(
    "Detokenization validation passed: No ERROR strings found in formatted column"
)



common.logger.info.assert_called_with("Detokenization validation passed: No ERROR strings found in formatted column")
@patch("utils.common.upper")
@patch("utils.common.col")
def test_validate_detokenized_dataframe_with_errors(self, mock_col, mock_upper):
    ...
