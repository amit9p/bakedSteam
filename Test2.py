
common.logger.info.assert_called_with("Detokenization validation passed: No ERROR strings found in formatted column")
@patch("utils.common.upper")
@patch("utils.common.col")
def test_validate_detokenized_dataframe_with_errors(self, mock_col, mock_upper):
    ...
