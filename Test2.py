

@patch("utils.common.upper")
@patch("utils.common.col")
def test_validate_detokenized_dataframe_exception(self, mock_col, mock_upper):
    mock_logger = MagicMock()
    common = Common(logger=mock_logger)

    mock_df = MagicMock()
    mock_df.columns = ["formatted", "other_column"]
    mock_df.filter.side_effect = Exception("DataFrame operation failed")

    mock_col.return_value = MagicMock()
    mock_upper.return_value = MagicMock()

    error_count = common.validate_detokenized_dataframe(mock_df, mock_logger)

    self.assertEqual(error_count, 0)
    mock_logger.error.assert_called()   # don’t hard match full string (too brittle)





@patch("utils.common.upper")
@patch("utils.common.col")
def test_validate_detokenized_dataframe_exception(self, mock_col, mock_upper):
    mock_logger = MagicMock()
    common = Common(logger=mock_logger)

    mock_df = MagicMock()
    mock_df.columns = ["formatted", "other_column"]
    mock_df.filter.side_effect = Exception("DataFrame operation failed")

    mock_col.return_value = MagicMock()
    mock_upper.return_value = MagicMock()

    with self.assertRaises(Exception):
        common.validate_detokenized_dataframe(mock_df, mock_logger)

    mock_logger.error.assert_called()
