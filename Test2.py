
from unittest.mock import MagicMock, patch

@patch("utils.common.upper")
@patch("utils.common.col")
def test_validate_detokenized_dataframe_no_errors(self, mock_col, mock_upper):
    mock_logger = MagicMock()
    common = Common(logger=mock_logger)

    # Mock dataframe + filter chain
    mock_df = MagicMock()
    mock_df.columns = ["formatted", "other_column"]

    mock_filtered_df = MagicMock()
    mock_filtered_df.count.return_value = 0
    mock_df.filter.return_value = mock_filtered_df

    # Mock Column expression chain: upper(col("formatted")).contains("ERROR")
    mock_col_expr = MagicMock()
    mock_upper_expr = MagicMock()
    mock_upper_expr.contains.return_value = MagicMock()  # filter condition

    mock_col.return_value = mock_col_expr
    mock_upper.return_value = mock_upper_expr

    error_count = common.validate_detokenized_dataframe(mock_df, mock_logger)

    self.assertEqual(error_count, 0)
    mock_logger.info.assert_called()   # keep loose unless log text is guaranteed
